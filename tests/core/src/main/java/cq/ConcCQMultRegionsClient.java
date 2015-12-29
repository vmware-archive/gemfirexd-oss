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

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;

import mapregion.MapBB;
import mapregion.MapPrms;
import util.SilenceListener;
import parReg.query.Position;
import util.NameFactory;
import util.TestException;
import util.TestHelper;
import hydra.*;

import java.util.*;

public class ConcCQMultRegionsClient extends MultRegionsClient {
  public static ConcCQMultRegionsClient concCQMultRegionsClient;
  protected int entryOperations; 
  static int verifyCount;
  protected int sleepMS = 10000;
  protected long minPerformEntryTime = 20000;  // duration of an entry operation task
  private int maxQty = 100;
  protected int maxPrice = 100;
  private int numOfSecurities = 200;
  private int numOfPutOpsByEdges;   //the number of put operation tasks to be done by edges when not verifying
  private int putCountByEdges = 0;
  protected boolean createIndex = false;
  
  public synchronized static void HydraTask_initialize() {
    if (concCQMultRegionsClient == null) {
      ConcCQTest.testInstance = new ConcCQTest();
      ConcCQAndOpsTest.concCQAndOpsTest = new ConcCQAndOpsTest();
      concCQMultRegionsClient = new ConcCQMultRegionsClient(); 
      verifyCount = TestConfig.tab().intAt(CQUtilPrms.edgeThreads);
      concCQMultRegionsClient.initialize();
    }   
  }
  
  protected void initialize() {
    try {
      entryOperations = TestConfig.tab().intAt(CQUtilPrms.feederThreads);
    } catch (HydraConfigException e) {
      entryOperations = TestConfig.tab().intAt(CQUtilPrms.edgeThreads);
      //either performed by feeders or edges
    }
    try {
      numOfPutOpsByEdges = TestConfig.tab().intAt(CQUtilPrms.edgeThreadsPerVM);
    } catch (HydraConfigException e) {
     //ignore, no need to set the varible
    }
    createIndex = TestConfig.tab().booleanAt(CQUtilPrms.createIndex, false);
    super.initialize();
  }
  
  public static void HydraTask_performEntryOperationsWithVerification()
  {
    concCQMultRegionsClient.performEntryOperationsWithVerification();
    // need to pause when verification is going on. 
    // The verification is performed in ConcCQAndOpsTest

    //  release any BridgeWriter/Loader connections per task invocation
    releaseConnections();
  }
  
  public static void HydraTask_performEntryOperationsWithoutVerification() {
    concCQMultRegionsClient.doEntry();  // no verification needed, so we do not need to pause.
  
    //  release any BridgeWriter/Loader connections per task invocation
    releaseConnections();
  } 
  
  //After number of entry operation tasks, one thread will signal when to verify 
  protected void performEntryOperationsWithVerification(){
    boolean verify = ((Boolean) CQUtilBB.getBB().getSharedMap().get(CQUtilBB.VerifyFlag)).booleanValue();
    boolean entryDone = ((Boolean) CQUtilBB.getBB().getSharedMap().get(CQUtilBB.EntryDone)).booleanValue();
    if (verify || entryDone) {
      MasterController.sleepForMs(sleepMS);
      return;
    }

    long entryPerformed = CQUtilBB.getBB().getSharedCounters().incrementAndRead(CQUtilBB.EntryPerformed);
    if (entryPerformed > entryOperations)  {
      return;
    }
    else if (entryPerformed == entryOperations) {
      CQUtilBB.getBB().getSharedMap().put(CQUtilBB.EntryDone, new Boolean(true));
      Log.getLogWriter().info("reset entryDone to true");
      doEntry();
      
      SilenceListener.waitForSilence(30, 5000);
      CQUtilBB.getBB().getSharedMap().put(CQUtilBB.VerifyFlag, new Boolean(true));  
      Log.getLogWriter().info("reset verifyFlag to true");
      CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.EntryPerformed);
      CQUtilBB.getBB().getSharedMap().put(CQUtilBB.EntryDone, new Boolean(false));
    }
    else  {
      doEntry();
    }
  }
      
  protected void doEntry() {  
    long startTime = System.currentTimeMillis();     
    do {
      super.performEntryOperations();
    } while (System.currentTimeMillis() - startTime < minPerformEntryTime);           
    
  }
  
  public static void HydraTask_putSameObject() {
    concCQMultRegionsClient.putSameObject();
  }
  
  protected void putSameObject() {
    String regionName = TestConfig.tab().stringAt(MapPrms.regionForOps);
    Region region = RegionHelper.getRegion(regionName);
    int numOfPutSameKey = 10;
    int fix = 1;
    
    Position pos = new Position();
    pos.init(fix);
    
    for (int i =0; i<numOfPutSameKey; i++) {
      region.put("Object_0", pos);
      Log.getLogWriter().info("put the same Object");
      MapBB.getBB().getSharedCounters().increment(MapBB.NUM_PUT);
      MasterController.sleepForMs(5000); //wait and put the same object
    }        
  }
 
  protected String thePrimarySecId = TestConfig.tab().stringAt(CQUtilPrms.primary, "0");
  protected boolean testPrimaryIndex = TestConfig.tab().booleanAt(CQUtilPrms.testPrimaryIndex, false);
  /**
   * put into the region and update blackboards for qualified put operation
   */
  protected void putObject(String regionName) {
    Region region = RegionHelper.getRegion(regionName);
    String name = NameFactory.getNextPositiveObjectName();
    Object anObj = getObjectToAdd(name);

    //Log.getLogWriter().info("addObject: calling put for name " + name + ", object " + 
    //       TestHelper.toString(anObj) +  ", region is " + region.getFullPath());
    try {
       region.put(name, anObj);
       Log.getLogWriter().info("put this key " + name + " and value " + anObj.toString());
       
       if (createIndex) {
         if (testPrimaryIndex && ((Position)anObj).getSecId().equals(thePrimarySecId)) {
           updateBBForPuts(regionName, name, anObj);
         }
       }
       else {
         updateBBForPuts(regionName, name, anObj);
       }

    } catch (RegionDestroyedException e) {
       handleRegionDestroyedException(region, e);
    } catch (Exception e) {
       throw new TestException(TestHelper.getStackTrace(e));
    }     
  }
  
  public static void HydraTask_invalidateSameKey() {
    String regionName = TestConfig.tab().stringAt(MapPrms.regionForOps);
    concCQMultRegionsClient.invalidateSameKey(regionName);
  }
  
  /**
   * to invalidate the sameKey.
   * @param regionName - region name for the region to perform invalidation
   */
  protected void invalidateSameKey(String regionName) {
    MasterController.sleepForMs(20000); //wait for a region to be populated
    Region region = RegionHelper.getRegion(regionName);
    int numOfInvalidateSameKey = 10;
    String key = null;
    if (region != null) {
      Set aSet = region.keySet();
      while (aSet == null) {
        aSet = region.keySet();
      }
      Iterator itr = aSet.iterator();
      if (itr.hasNext()) {
        key = (String)(itr.next());
      }
    }
    if (key != null) {
      for (int i=0; i<numOfInvalidateSameKey; i++) {
        Log.getLogWriter().info("the key to be invalidated is "+key);
        region.invalidate(key);
        MapBB.getBB().getSharedCounters().increment(MapBB.NUM_INVALIDATE);
        MasterController.sleepForMs(5000); //wait to invalidate the same key
      }
    }
  }
  
  /**
   * To update MapBB for NUM_PUT and ConcCQBB for snapshots if the put object qualifies
   * for the query criteria
   * @param regionName
   * @param name
   * @param anObj
   */
  protected void updateBBForPuts(String regionName, String name, Object anObj ) {
    MapBB.getBB().getSharedCounters().increment(MapBB.NUM_PUT);   
   
    String key = null;
    if (testPrimaryIndex) key = regionName + "_Object_" + name; //secId is Object_xxx for testingPrimaryIndex
    else key = regionName + "_" + name;
    ConcCQBB.getBB().getSharedMap().put(key, anObj);
    
  }
  /**
   * regular object that has qty of multiple of 100
   * @param name key for put into the region
   * @return a Position
   */
  protected Object getObjectToAdd(String name) {
    if (testPrimaryIndex) {
      return getObjectToAddForPrimaryIndex(name);
    }
        
    int counter = (int) NameFactory.getCounterForName(name);
    Properties props = new Properties();
    
    Double qty = new Double(rnd.nextInt(maxQty)* 100);
    Double mktValue = new Double(rnd.nextDouble() * maxPrice);
    Integer secId = new Integer(counter%numOfSecurities);
    
    props.setProperty("qty", qty.toString());
    props.setProperty("secId", secId.toString());
    props.setProperty("mktValue", mktValue.toString());
    
    Position aPosition = new Position();
    aPosition.init(props);
            
    return aPosition;
  }

  /**
   * generate object with primary key on secId
   * @param name unique object name used to generate primary key
   * @return a Position
   */
  protected Object getObjectToAddForPrimaryIndex(String name) {
    Properties props = new Properties();
    
    Double qty = new Double(rnd.nextInt(maxQty)* 100);
    Double mktValue = new Double(rnd.nextDouble() * maxPrice);

    
    props.setProperty("qty", qty.toString());
    props.setProperty("secId", name);
    props.setProperty("mktValue", mktValue.toString()); 
    
    Position aPosition = new Position();
    aPosition.init(props);
            
    return aPosition;
  }
  
  protected void putAllObjects(String regionName) {
    putObject(regionName);
  }
  
  protected void removeObject(String regionName) {
    String key = null;
    long counter = CQUtilBB.getBB().getSharedCounters().incrementAndRead(CQUtilBB.NumCounterToDestroy);

    Region region = RegionHelper.getRegion(regionName);
    if (region != null) {        
      int temp = rnd.nextInt(2);
      key = (temp>0) ? thePrimarySecId : NameFactory.getObjectNameForCounter(counter);
      //key = NameFactory.getObjectNameForCounter(counter);
      if (counter <= 100) {
        MasterController.sleepForMs(1000);
      } //to avoid bug 35662 -- allow the put key event propogated to the p2p bridges first before destory 
      if (counter <= 100) {
        MasterController.sleepForMs(1000);
      } //to avoid bug 35662 -- allow the put key event propogated to the p2p bridges first before destory 
      if (counter <= 100) {
        MasterController.sleepForMs(1000);
      } //to avoid bug 35662 -- allow the put key event propogated to the p2p bridges first before destory 
      Object got = region.get(key); 
      if (got != null) {
        region.remove(key);
        Log.getLogWriter().info("successfully removed this key: " + key + " and value " + got.toString());
        if (createIndex) {
          if (testPrimaryIndex  && ((Position)got).getSecId().equals(thePrimarySecId)) {
            updateBBForRemoves(regionName, key);
          }
        }
        else {
          updateBBForRemoves(regionName, key);
        }
     }
    }    
  }
  
  protected void updateBBForRemoves(String regionName, String key) {
    MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_REMOVE);
   
    if (testPrimaryIndex) key = regionName + "_Object_" + key; //secId is Object_xxx for testingPrimaryIndex
    else key = regionName + "_" + key; //new key for sharedMap used for verification
    
    //this only works for one destroy on a single key
    long start = System.currentTimeMillis();
    long waitMs = 10000;
    do {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Log.getLogWriter().info("ignore InterruptedException during waiting for putting in snapshot");
      }
    } while (ConcCQBB.getBB().getSharedMap().get(key) == null && System.currentTimeMillis() - start < waitMs  );
    
    if (ConcCQBB.getBB().getSharedMap().get(key) != null) {
      ConcCQBB.getBB().getSharedMap().remove(key); 
      Log.getLogWriter().info("removed from snapshots the key " + key);
    }
    else {
      throw new TestException ("the object (" + key + ") to be removed is not in the snapshots for " + waitMs/1000 + " second");
    }
  }
  
  protected void removeObject() {   

    String key = null;
    String[] regionNames = MapPrms.getRegionNames();
    long counter = CQUtilBB.getBB().getSharedCounters().incrementAndRead(CQUtilBB.NumCounterToDestroy);
    // no duplicate removal during the test, 
    
    for (int i = 0; i < regionNames.length; i++) {
      String regionName = regionNames[i];
      Region region = RegionHelper.getRegion(regionName);
      if (region != null) {        
        key = NameFactory.getObjectNameForCounter(counter);
        Object got = region.get(key); 
        if (got != null) {
          region.remove(key);
          Log.getLogWriter().info("successfully removed this key: " + key);
          MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_REMOVE);
          return;
       }
      }
    }    
  }
  
  public static void feedData()
  {
    concCQMultRegionsClient.loadData();
    
    //  release any BridgeWriter/Loader connections per task invocation
    releaseConnections();
  }
  
  protected void loadData(){
    long startTime = System.currentTimeMillis();     
    do {
      super.loadData();
    } while (System.currentTimeMillis() - startTime < minPerformEntryTime);   
    // performed when not verify initial result in the ConcCQAndOpsTest
  }
  
  public static void feedDataWithVerification() {
    concCQMultRegionsClient.loadDataWithVerification();
    // performed when verifing initial result occurs in the ConcCQAndOpsTest.
  }
  
  protected void loadDataWithVerification() {
    //  performed when verifing initial result occurs in the ConcCQAndOpsTest.
    // need to pause when verification is going on

    boolean verify = ((Boolean) CQUtilBB.getBB().getSharedMap().get(CQUtilBB.VerifyFlag)).booleanValue();
    if (!verify) {
      if (putCountByEdges < numOfPutOpsByEdges) {
        putCountByEdges ++;
        loadData();
      }
      else if (putCountByEdges== numOfPutOpsByEdges) {
        putCountByEdges++;
        loadData();
        verify = ((Boolean) CQUtilBB.getBB().getSharedMap().get(CQUtilBB.VerifyFlag)).booleanValue();
        while (!verify) {
          try {
            Thread.sleep(sleepMS);
            verify = ((Boolean) CQUtilBB.getBB().getSharedMap().get(CQUtilBB.VerifyFlag)).booleanValue();
          } catch (InterruptedException e) {
            Log.getLogWriter().info("Got InterruptedException while sleeping");
          }
        }
        putCountByEdges = 0;        //reset the count value when we need to verify
      }
      else return;
    }
    
  }
  
  protected void handleRegionDestroyedException(Region aRegion, RegionDestroyedException anException) {
     // make sure the region destroyed is this region
     if (!anException.getRegionFullPath().equals(aRegion.getFullPath())) {
       TestException te = new TestException("Got a RegionDestroyedException when operating on region " +
       TestHelper.regionToString(aRegion, false) + ", but the region destroyed is '" +
       anException.getRegionFullPath() +"'");
       te.initCause(anException);
       throw te;
     }

     // Note: the test only creates a region with a given name once. Once that region
     // has been destroyed, the test will never create another region with the same name
     boolean isDestroyed = aRegion.isDestroyed();
     if (isDestroyed) {
        // Make sure it really is destoyed and is not causing the RegionDestroyedException to be
        // thrown because one of its subregions was destroyed. 
        Log.getLogWriter().info("Got " + RegionDestroyedException.class.getName() + 
            " on " + TestHelper.regionToString(aRegion, false) + "; exception expected, continuing test");
     } else { // the region was not destroyed, but we got RegionDestroyedException anyway
        throw new TestException("Bug 30645 (likely): isDestroyed returned " + isDestroyed + " for region " +
           TestHelper.regionToString(aRegion, false) + ", but a region destroyed exception was thrown: " +
           TestHelper.getStackTrace(anException));
     }
  }
} 
  

