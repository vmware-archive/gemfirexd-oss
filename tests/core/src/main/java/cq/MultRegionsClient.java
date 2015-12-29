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
 * Created on 5/22/07 since 5.1 Beta
 * 
 * Aim:- The aim of this test to validate the region features with multiple
 * diskregions in a single vm
 */
package cq;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import mapregion.MapBB;
import mapregion.MapPrms;
import objects.ObjectHelper;
import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.Log;
import hydra.MasterController;
import hydra.PoolHelper;
import hydra.RegionHelper;
import hydra.TestConfig;
import util.NameBB;
import util.NameFactory;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.query.CqQuery;

public class MultRegionsClient {
  protected static MultRegionsClient testInstance;
  protected Boolean regionOpsTryLock = new Boolean(false); 
  //this requires only one vm performing both entry and region ops  
  //to avoid performing putAll ops and region ops at the same time
  
  protected Integer numOfCurrentPutAllOps =  new Integer(0);
  protected boolean performedDestroyRegion = false;
  protected boolean performedInvalidateRegion = false;
  protected boolean performedClearRegion = false;
  protected boolean cqsOn = true;
  protected static Random rnd = new Random();
  protected int lowerThreshold;           //Value of MapRegion.lowerThreshold
  protected int upperThreshold;           //Value of MapRegion.upperThreshold 


  public synchronized static void HydraTask_initialize()
  {
    if (testInstance == null) {
      ConcCQTest.testInstance = new ConcCQTest();
      testInstance = new MultRegionsClient();
      testInstance.initialize();
    }
  }

  protected void initialize()
  {
    try {
      CacheHelper.createCache(ConfigPrms.getCacheConfig());

      RegionAttributes attr = RegionHelper.getRegionAttributes(ConfigPrms
          .getRegionConfig());

      String[] regionNames = MapPrms.getRegionNames();
      
      regions = new Region[regionNames.length];
      for (int i = 0; i < regionNames.length; i++) {
        regions[i] = RegionHelper.createRegion(regionNames[i], attr);        
        if (regions[i].getAttributes().getDataPolicy().withPartitioning()) {
           if (regions[i].getAttributes().getPartitionAttributes().getRedundantCopies() > 0) {
              ConcCQBB.getBB().getSharedMap().put("expectRecovery", new Boolean(true));
           }
        }
      }
      ConcCQBB.getBB().getSharedMap().put("numPRs", new Integer(regionNames.length));
      
      // Check if QueryService needs to be obtained from Pool.
      String usingPool = TestConfig.tab().stringAt(CQUtilPrms.QueryServiceUsingPool, "false");
      boolean queryServiceUsingPool = Boolean.valueOf(usingPool).booleanValue();
      if (queryServiceUsingPool){
        Pool pool = PoolHelper.createPool(CQUtilPrms.getQueryServicePoolName());
        ConcCQTest.cqService = pool.getQueryService();
        Log.getLogWriter().info("Initializing QueryService using Pool. PoolName: " + pool.getName());
      } else {
        ConcCQTest.cqService = CacheHelper.getCache().getQueryService();
        Log.getLogWriter().info("Initializing QueryService using Cache.");
      }

      cqsOn = TestConfig.tab().booleanAt(CQUtilPrms.CQsOn, true); //default CQsOn is true

      if (cqsOn) {
        registerCQs();
      } //to determine if the failure is related to CQ
      CQUtilBB.getBB().getSharedMap().put(CQUtilBB.PerformedDestroyRegion, new Boolean (false));
      CQUtilBB.getBB().getSharedMap().put(CQUtilBB.PerformedInvalidateRegion, new Boolean (false));
    }
    catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }
  
  /**
   * for edges to registerCQs
   *
   */
  protected void registerCQs() {
    if (!ConfigPrms.getRegionConfig().equalsIgnoreCase("feed")) {
      if (MapPrms.getRegisterAndExecuteCQs()) {
        Log.getLogWriter().info("registering and executing CQs");
        ConcCQTest.testInstance.registerAndExecuteCQs();
      }
      else {
        Log.getLogWriter().info("registering CQs");
        ConcCQTest.testInstance.registerCQs();
      }
    }
  }
  
  /**
   * Hydra task to perform region operation -- destroyRegion.
   *
   */
  public static void HydraTask_destroyRegion() {
    testInstance.destroyRegion();
  }
 
  /**
   * to specify a region for region ops.
   *
   */
  protected void destroyRegion() {
    String regionName = TestConfig.tab().stringAt(MapPrms.regionForOps);
    destroyRegion(regionName);
  }
  
  
  /**
   * perform destroy region operation on the specified region.
   * @param regionName - used to determine the region performing region ops
   */
  protected void destroyRegion(String regionName){
    Region region = RegionHelper.getRegion(regionName);
    
    MasterController.sleepForMs(20000); //sleep 20sec before destroy a region
    synchronized (regionOpsTryLock) {
      regionOpsTryLock = new Boolean(true);      
    }
    
    while (true){
      synchronized (numOfCurrentPutAllOps) {
        if (numOfCurrentPutAllOps.intValue() == 0) {
          if (region != null) {
            region.destroyRegion();
            Log.getLogWriter().info("Successfully destroyed the region " + regionName);
            synchronized (regionOpsTryLock) {
              regionOpsTryLock = new Boolean(false);      
            } //for putAll ops and other region ops to proceed
            CQUtilBB.getBB().getSharedMap().put(CQUtilBB.PerformedDestroyRegion, new Boolean (true));
            performedDestroyRegion = true;
            MapBB.getBB().getSharedCounters().increment(MapBB.NUM_CLOSE);
            return;
          } else {
            //only perform destroyRegion once in the test.
            throw new TestException("region " + regionName + " is not created properly" );
          }          
        }         
      } //end synchronized numOfCurrentPutAllOps 
      MasterController.sleepForMs(5000); //yield for putAll ops to be completed
    }
  }
  
  /**
   * hydraTask to perform one of the following two region operations: region.clear() and
   * region.invalidateRegion().
   */
  public static void HydraTask_serialRegionOps() {
    testInstance.performRegionOperations();
  }
  
  static protected final int CLEAR_REGION = 10;

  static protected final int INVALIDATE_REGION = 11;

  /**
   * to determine which region operation to perform based on MapPrms.regionOperationName
   * @param whichPrm - determines which region operation
   * @return an int to be used to determine region operation
   */
  protected int getRegionOperation(Long whichPrm){
    int op = 0;
    String operation = TestConfig.tab().stringAt(whichPrm);

    if (operation.equals("clearRegion"))
      op = CLEAR_REGION;
    else if (operation.equals("invalidateRegion"))
      op = INVALIDATE_REGION;
    else
      throw new TestException("Unknown entry operation: " + operation);
    return op;
  }
  
  /**
   * perform one of the region operations.
   * 
   */
  protected void performRegionOperations(){
    try {
      int whichOp = getRegionOperation(MapPrms.regionOperationName);
      switch (whichOp) {
        case CLEAR_REGION:
          clearRegion();
          break;
        case INVALIDATE_REGION:
          invalidateRegion();
          break;
        default: {
          throw new TestException("Unknown region operation " + whichOp);
        }
      }
    }
    catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }
   
  /**
   * to specify a region for region ops.
   *
   */
  protected void clearRegion() {      
    String regionName = TestConfig.tab().stringAt(MapPrms.regionForOps);
    clearRegion(regionName);
  } //clearRegion will be used in serial test
  
  /**
   * clearRegion will be used in serial test to clear all the entries in the specified region
   * there will be only one region operation in any single test run
   * @param regionName - specified region to be cleared
   */
  protected void clearRegion(String regionName) {
    Region region = RegionHelper.getRegion(regionName);    
    
    if (region != null) {
      if (region.entrySet().size() < 100) {
        Log.getLogWriter().info("Not enough entries in the region to be cleared.");
        return;
      } // wait until there are some entries in the region to be cleared
      else {
        int destroyed = region.entrySet().size();
        region.clear();
        Log.getLogWriter().info("clearRegion destroys " + destroyed + " entries in region " + regionName);
        MapBB.getBB().getSharedCounters().increment(MapBB.NUM_REGION_DESTROY); //update num of clearRegion
      }
    }
  }    
  
  public static void HydraTask_executeCQ() {
    testInstance.executeCQ();
  }
  
  /**
   * to test what happens if invoke CQs after destroyRegion(). 
   * 
   */
  protected void executeCQ() {
    boolean exeCq = ((Boolean)CQUtilBB.getBB().getSharedMap().get(CQUtilBB.PerformedDestroyRegion)).booleanValue();
    if (exeCq){
      edgeExecuteCQ();
    }
  }
  
  protected void edgeExecuteCQ() {
    CqQuery cqArray[] = ConcCQTest.cqService.getCqs();
    Log.getLogWriter().info("The number of CqQuery in cqArray is "+ cqArray.length);
    CqQuery cq = null;
    for (int i=0; i<cqArray.length; i++) {
      cq = cqArray[i];
      try {
        cq.stop();
      } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      ConcCQTest.testInstance.executeWithInitialResultsCQ(cq);
      Log.getLogWriter().info("executed cq for " + cqArray[i].getQuery());
    }
  }
  
  /**
   * hydra task to perform invalidate region ops.
   *
   */
  public static void HydraTask_invalidateRegion() {
    testInstance.invalidateRegion();
  }
  
  /**
   * to specify a region for region ops.
   *
   */
  protected void invalidateRegion() {
    String regionName = TestConfig.tab().stringAt(MapPrms.regionForOps);
    invalidateRegion(regionName);    
  } //invalidateRegion will be used in serial test
  
  /**
   * perform invalidateRegion operation on the specified region.
   * @param regionName - used to determine the region performing region ops
   */
  protected void invalidateRegion(String regionName) {
    Region region = RegionHelper.getRegion(regionName);    
    
    if (region != null) {
      if (region.entrySet().size() < 100) {
        Log.getLogWriter().info("Not enough entries in the region to be invalidated.");
        return;
      } // wait until there are some entries in the region to be invalidated
      else {
        int invalidated = region.entrySet().size();
        region.invalidateRegion();
        Log.getLogWriter().info("invalidateRegion invalidates " + invalidated + " entries in region " + regionName);
        MapBB.getBB().getSharedCounters().increment(MapBB.NUM_REGION_INVALIDATE); //update num of invalidateRegion
      }
    }   
  }
  
  public static void HydraTask_performEntryOperations()
  {
    String regionName = TestConfig.tab().stringAt(MapPrms.regionForOps);
    testInstance.performEntryOperations(regionName);
    
    releaseConnections();
  }

  static protected final int PUT = 1;

  static protected final int PUT_ALL = 2;

  static protected final int REMOVE = 3;

  static protected final int INVALIDATE = 4;

  protected int getEntryOperation(Long whichPrm)
  {
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
    else
      throw new TestException("Unknown entry operation: " + operation);
    return op;
  }
  
  protected void performEntryOperations() {
    String regionName = TestConfig.tab().stringAt(MapPrms.regionForOps);
    performEntryOperations(regionName);
    
  }
 
  //to release connection from edges after each hydra task.
  protected static void releaseConnections() {
    String regionName = TestConfig.tab().stringAt(MapPrms.regionForOps);   
    Region aRegion = RegionHelper.getRegion(regionName);
    while (aRegion == null) {
      regionName = TestConfig.tab().stringAt(MapPrms.regionForOps);   
      aRegion = RegionHelper.getRegion(regionName);
    } //region ops may have destroyed the region
    
    ClientHelper.release(aRegion);
  }

  protected void performEntryOperations(String regionName)
  {
    try {
      int whichOp = getEntryOperation(MapPrms.entryOperationName);
      int size = 0;

      lowerThreshold = TestConfig.tab().intAt(MapPrms.lowerThreshold, -1);
      upperThreshold = TestConfig.tab().intAt(MapPrms.upperThreshold, Integer.MAX_VALUE);

      Log.getLogWriter().info("lowerThreshold " + lowerThreshold + ", " + "upperThreshold " + upperThreshold );
      Region aRegion = RegionHelper.getRegion(regionName);
      
      if (aRegion == null) 
          return;

      if (aRegion.getAttributes().getPoolName() != null) {
          size = aRegion.keySetOnServer().size(); 
      } else {
          size = aRegion.size();
      }

      Log.getLogWriter().info(" Region.size= " + size);

      if (size >= upperThreshold) {
         Log.getLogWriter().info("Above the upperThreshold  Region.size= " + size);
         whichOp = getEntryOperation(MapPrms.upperThresholdOperations);
      }else if (size <= lowerThreshold) {
        Log.getLogWriter().info("Below the lowerThreshold  Region.size= " + size); 
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
      default: {
        throw new TestException("Unknown operation " + whichOp);
      }
      }
    }
    catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }
  
  protected void putObject(String regionName)
  {
    try {
      long limit = TestConfig.tab().longAt(MapPrms.timeForPutOperation);
      Object key = null, val = null;
      String objectType = MapPrms.getObjectType();
      int putKeyInt, numPosEntries;
      Region region = RegionHelper.getRegion(regionName);
      if (region != null) {
        long startTime = System.currentTimeMillis();
        do {
          numPosEntries = (int)NameFactory.getPositiveNameCounter();
          if(numPosEntries > MapPrms.getMaxPositiveKeys()){
            NameBB.getBB().zero ("NameBB.POSITIVE_NAME_COUNTER", NameBB.POSITIVE_NAME_COUNTER);    
          }
          key = NameFactory.getNextPositiveObjectName();
          putKeyInt = (int)NameFactory.getCounterForName(key);
          val = ObjectHelper.createObject(objectType, putKeyInt);
          region.put(key, val); 
          Log.getLogWriter().info("put this key: " + key + " in " + regionName);
          MapBB.getBB().getSharedCounters().increment(MapBB.NUM_PUT);
        } while ((System.currentTimeMillis() - startTime) < limit);
      }
    } catch (RegionDestroyedException rde) {
      Log.getLogWriter().info("RegionDestroyedException ...may occur in concurrent execution. Continuing with test.");
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }
  
  protected void putAllObjects(String regionName)
  {
    Object key = null, val = null;    
    String objectType = MapPrms.getObjectType();
    int putAllKeyInt, numNegEntries;  
    int minQty = 50;
    int maxQty = 100;
    int numForPutAll = rnd.nextInt(maxQty - minQty) + minQty;
    Map m = new TreeMap();

    
    Region region = RegionHelper.getRegion(regionName); 
    
    if (region != null) {
      synchronized (regionOpsTryLock) {
        //  do not perform putAll operation when another thread trys to perform region ops
        if (regionOpsTryLock.booleanValue()) return; 
        
        synchronized (numOfCurrentPutAllOps) {
          numOfCurrentPutAllOps = new Integer(numOfCurrentPutAllOps.intValue()+1) ;
        } //end synchronized numOfCurrentPutAllOps
      } // end synchronized regionOpsTryLock
      
      for (int i = 0; i < numForPutAll; i++) {
        numNegEntries = Math.abs((int)NameFactory.getNegativeNameCounter());
        if(numNegEntries > MapPrms.getMaxNagetiveKeys()){
          NameBB.getBB().zero("NameBB.NEGATIVE_NAME_COUNTER", NameBB.NEGATIVE_NAME_COUNTER);        
        }
        key = NameFactory.getNextNegativeObjectName();
        putAllKeyInt = (int)NameFactory.getCounterForName(key);
        val = ObjectHelper.createObject(objectType, putAllKeyInt);
        m.put(key, val);
        Log.getLogWriter().info("put this key " + key + " in the map for putAll");
      } //end for loop 

      try {
        Log.getLogWriter().info("--- executing putAll with mapSize = " + m.size());
        region.putAll(m);
        MapBB.getBB().getSharedCounters().add(MapBB.NUM_PUT, m.size()); 
        //MapBB.NUM_PUT will be increamented once for the size of the map
        Log.getLogWriter().info("----performed putAll operation on region " + regionName);
      } catch (Exception ex) {
        throw new TestException(TestHelper.getStackTrace(ex));
      } //region ops can't mix with putAll operation in the test
      synchronized (numOfCurrentPutAllOps) {
        numOfCurrentPutAllOps = new Integer(numOfCurrentPutAllOps.intValue()-1) ;
      } //end synchronized

    }
  }  
  
  protected static Region[] regions;
  
  protected void removeObject(String regionName) {
    Object keyToDestroy = null;
    int removeCntr;
    Object got;
    
    try {
      long limit = TestConfig.tab().longAt(MapPrms.timeForRemoveOperation);

      long startTime = System.currentTimeMillis();
      do {
        removeCntr = rnd.nextInt(MapPrms.getMaxPositiveKeys()) + 1;
        keyToDestroy = NameFactory.getObjectNameForCounter(removeCntr);  
        synchronized (regions) {
          for (int i =0; i<regions.length; i++) {
            if (regions[i].getName().equalsIgnoreCase(regionName)) {
              got = regions[i].getEntry(keyToDestroy);
              if (got != null) {
                regions[i].remove(keyToDestroy);
                Log.getLogWriter().info("removed " + keyToDestroy + " in " + regionName);
                MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_REMOVE);
                break;
              }  
            }
          }      
        }
      } while ((System.currentTimeMillis() - startTime) < limit);      

    } catch (RegionDestroyedException rde) {
      Log.getLogWriter().info("RegionDestroyedException ...may occur in concurrent execution. Continuing with test.");
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }  

  protected void invalidateObject(String regionName)
  {
    Object keyToInvalidate = null;
    int invalidateCounter;
    Object got;
    
    try {
      long limit = TestConfig.tab().longAt(MapPrms.timeForRemoveOperation);

      long startTime = System.currentTimeMillis();
      do {
        invalidateCounter = rnd.nextInt(MapPrms.getMaxNagetiveKeys()) + 1;
        keyToInvalidate = NameFactory.getObjectNameForCounter(invalidateCounter * -1);  
        synchronized (regions) {
          for (int i =0; i<regions.length; i++) {
            if (regions[i].getName().equalsIgnoreCase(regionName)) {
              got = regions[i].get(keyToInvalidate);
              if (got != null) {
                regions[i].invalidate(keyToInvalidate);
                Log.getLogWriter().info("Invalidated " + keyToInvalidate + " in " + regionName);
                long temp = MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_INVALIDATE);
                Log.getLogWriter().info("The number of invalidated entries is " + temp );
                break;
              }  
            }
          }      
        }
      } while ((System.currentTimeMillis() - startTime) < limit);     
    } catch (EntryNotFoundException enfe) {
      Log.getLogWriter().info("EntryNotFoundException ...may occur in concurrent execution. Continuing with test.");
    } catch (RegionDestroyedException rde) {
      Log.getLogWriter().info("RegionDestroyedException ...may occur in concurrent execution. Continuing with test.");
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }   //changed implementation of invalidateObjects to elimnate the duplicate invalidates

  public static void feedData()
  {   
    testInstance.loadData();
    // release any BridgeWriter/Loader connections per task invocation
    releaseConnections();
    
  }

  protected void loadData()
  {
    String[] regionNames = MapPrms.getRegionNames();
    for (int i = 0; i < regionNames.length; i++) {
      Log.getLogWriter().info("feeding data into region:- " + regionNames[i]);
      //putObject(regionNames[i]);
      putAllObjects(regionNames[i]);
      Log.getLogWriter().info("data fed into region:- " + regionNames[i]);
    }
  }
  
}
