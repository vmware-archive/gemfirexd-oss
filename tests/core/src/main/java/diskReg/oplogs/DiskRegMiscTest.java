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
package diskReg.oplogs;

import hydra.*;
import util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.*;
import distcache.gemfire.GemFireCachePrms;
import mapregion.MapPrms;
import objects.ObjectHelper;

/**
 * The aim of this test is to make sure that async disk writer writes as
 * expected. Buffer size and time are so adjusted that few coflicting entry and
 * region operations are combined in one buffer. After that get is performed to
 * make sure that expected values are returned.
 * 
 * @author prafulla
 * @since 5.1
 */

public class DiskRegMiscTest
{
  static DiskRegMiscTest testInstance;

  static private RegionAttributes attr;

  static private Cache cache;  
  
  static protected boolean isCarefulValidation = false;

  static Region diskRegion = null;
  
  static String regionName;
  
  static String DEFAULT_UPDATE_STRING = "updated";
  

  // //////constructor

  public static synchronized void HydraTask_initialize()
  {
    // 1. create regions
    if (testInstance == null) {
      testInstance = new DiskRegMiscTest();
      testInstance.initialize();
    }
  }// end of HydraTask_initialize

  protected void initialize()
  {
    try {
      if (cache == null || cache.isClosed()) {
    	  cache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
      }
      
      TestHelperForHydraTests.setIssueCallbacksOfCacheObserver(true);
      
      attr = RegionHelper.getRegionAttributes(ConfigPrms.getRegionConfig());
      regionName = RegionHelper.getRegionDescription(ConfigPrms.getRegionConfig()).getRegionName();
      
      createDiskRegion();
      
      if(diskRegion != null){
        startMonitorThread();
      }
      
      if(TestConfig.tab().booleanAt(MapPrms.populateCache)){
        populateRegion();
      }
      
      ////  this parameter @link serialExecution decides for careful validations of the region methods e.g. put
      isCarefulValidation = TestConfig.tab().booleanAt(Prms.serialExecution);
      
    }
    catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }

  protected void createDiskRegion()
  {
    try {
      diskRegion = RegionHelper.createRegion(regionName, attr);
    }
    catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }
  
  protected void startMonitorThread(){
    Thread monitorThread = new Thread(new Runnable() {
      public void run()
      {
        long  limit =  ( TestConfig.tab().longAt(Prms.totalTaskTimeSec) - 10 );
        long startTime = System.currentTimeMillis();
        
        while (System.currentTimeMillis() < (startTime + (limit * 1000) ) ) {
          int numKeys = diskRegion.size();
          Log.getLogWriter().info(diskRegion.getFullPath() + " has " + numKeys + " keys");
          try {
            Thread.sleep(5000);
          }
          catch (InterruptedException e) {
          }
        }
      }
    });
    
    monitorThread.start();
  }//end of startMonitorThread

  /**
   * Performs the sequence of operations and validate the values after that.
   */
  public static void HydraTask_PerformOperations()
  {
    //perform operations on entries to validate that async thread is writing expected values to disk.
    testInstance.createModifyModify();
    testInstance.createDestroyCreate();
    testInstance.createRemoveCreateModify();
    
  }// end of HydraTask_PerformOperations

  /**
   * Creates - modifies - the entry.
   */
  static volatile int createKeyInt=0;
  static Object createKeyLock = "createKey";
  
  protected void createModifyModify()
  {
   try {
     Object key=null, val=null;
     String objectType = MapPrms.getObjectType();
     
     synchronized (createKeyLock) {
       key = ObjectHelper.createName(createKeyInt);       
       val = ObjectHelper.createObject(objectType, createKeyInt);
       createKeyInt++;
    }
     diskRegion.put(key, val);
     
     val = val.toString().concat("-updated-1");
     
     diskRegion.put(key, val);
     
     if (isCarefulValidation){
       Object got = diskRegion.get(key);
       Log.getLogWriter().info("Got value after first update: "+got.toString());
       if( ! got.toString().endsWith("updated-1")){
         throw new TestException ("Value is not updated by async writer thread. Expected to end with: \"updated-1\". But is: "+got.toString());
       }
     }
     
     val = val.toString().concat("-updated-2");
     
     diskRegion.put(key, val);
     
     if (isCarefulValidation){
       Object got = diskRegion.get(key);
       Log.getLogWriter().info("Got value after second update: "+got.toString());
       if( ! got.toString().endsWith("updated-2")){
         throw new TestException ("Value is not updated by async writer thread. Expected to end with: \"updated-2\". But is: "+got.toString());
       }
     }
     
    }
    catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }
  
  protected void createDestroyCreate()
  {
   try {
     Object key=null, val=null;
     String objectType = MapPrms.getObjectType();
     
     synchronized (createKeyLock) {
       key = ObjectHelper.createName(createKeyInt);       
       val = ObjectHelper.createObject(objectType, createKeyInt);
       createKeyInt++;
    }
     diskRegion.put(key, val);
     
     diskRegion.destroy(key);
     
     diskRegion.put(key, val);
     
     if (isCarefulValidation){
       Object got = diskRegion.get(key);
       if(got == null){
         throw new TestException("Value obtained is null after create destroy create conflation");
       }
       ObjectHelper.validate( Integer.parseInt( (String)key), got);
     }
     
    }
    catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }
  
  protected void createRemoveCreateModify()
  {
   try {
     Object key=null, val=null;
     String objectType = MapPrms.getObjectType();
     
     synchronized (createKeyLock) {
       key = ObjectHelper.createName(createKeyInt);       
       val = ObjectHelper.createObject(objectType, createKeyInt);
       createKeyInt++;
    }
     diskRegion.put(key, val);
     
     diskRegion.remove(key);
     
     diskRegion.put(key, val);
     
     if (isCarefulValidation){
       Object got = diskRegion.get(key);
       if(got == null){
         throw new TestException("Value obtained is null after create remove create conflation");
       }
       ObjectHelper.validate( Integer.parseInt( (String)key), got);
     }
     
     val = val.toString().concat("-updated-1");
     
     diskRegion.put(key, val);
     
     if (isCarefulValidation){
       Object got = diskRegion.get(key);
       Log.getLogWriter().info("Got value after first update: "+got.toString());
       if( ! got.toString().endsWith("updated-1")){
         throw new TestException ("Value is not updated by async writer thread. Expected to end with: \"updated-1\". But is: "+got.toString());
       }
     }
     
    }
    catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }
  
  /**
   * Perform the extesive get on already populated htree and oplogs of the region.
   * Validate that all entries have expected values. 
   */
  
  public static void HydraTask_PerformGetOperations()
  {    
    testInstance.getEntries();    
  }// end of HydraTask_PerformGetOperations
  
  protected volatile boolean hasBeenRolled = false;
  protected volatile boolean hasBeenFlushed = false;
  static int iteration = 0;//temporarily kept...will look into it.
  protected void getEntries(){
    try {
      hasBeenRolled = false; hasBeenFlushed = false;
      Object key = null, val = null, got = null;      
      Log.getLogWriter().info("Region size is: "+diskRegion.size());
      iteration++;
      int getKeyInt = 0;
      int keyInt = (int) DiskBB.getBB().getSharedCounters().read(DiskBB.NUM_GET);
      //default object size used in put operation is 1kb.
      int queueSize = DiskStoreHelper.getDiskStore(attr.getDiskStoreName()).getQueueSize();
      
      do{
        getKeyInt++;
        keyInt++;
        if(keyInt> populateKeyInt){
          keyInt = 1;
        }
        key = ObjectHelper.createName(keyInt);
        got = ( (LocalRegion)diskRegion ).getValueOnDiskOrBuffer(key);
        
        val = got.toString().concat(DEFAULT_UPDATE_STRING)+Integer.toString(iteration);
        diskRegion.put(key, val);
        
        got = ( (LocalRegion)diskRegion ).getValueOnDiskOrBuffer(key);
        
        //values will be taken from buffer
        if(got.toString().indexOf(DEFAULT_UPDATE_STRING) == -1){
          throw new util.TestException ("Value obtained from buffer is "+got.toString()+ " whereas it is expected to be "+val.toString());
        }
        
      } while(getKeyInt < ( queueSize / 2) );      
      
      //flush so that the data will go into oplog
      
      CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        public void afterWritingBytes() {
          synchronized (diskRegion) {
            diskRegion.notify();
            hasBeenFlushed = true;
            Log.getLogWriter().info("inside afterWritingBytes");
          }
        }
      });
     
      ( (LocalRegion)diskRegion ).getDiskRegion().flushForTesting();  
      
      synchronized(diskRegion) {
        if (!hasBeenFlushed) {
          try {
            diskRegion.wait();
          }
          catch (InterruptedException ex) {
            throw new util.TestException(TestHelper.getStackTrace(ex));
          }
        }
      }
      
       Log.getLogWriter().info("flushed the data into oplogs");
       
       getKeyInt = 0;
       keyInt = (int) DiskBB.getBB().getSharedCounters().read(DiskBB.NUM_GET);       
       do{
         getKeyInt++;
         keyInt++;
         if(keyInt> populateKeyInt){
           keyInt = 1;
         }
         key = ObjectHelper.createName(keyInt);       
        
         val = diskRegion.get(key);//for persist only mode the value will be returned from vm
         
         got = ( (LocalRegion)diskRegion ).getValueOnDiskOrBuffer(key);
         
         //values will be taken from oplogs
         if(got.toString().indexOf(DEFAULT_UPDATE_STRING) == -1){
           throw new util.TestException ("Value obtained from oplogs is "+got.toString()+ " whereas it is expected to be "+val.toString());
         }
         
       } while(getKeyInt < ( queueSize / 2) );
       
       CacheObserverHolder.setInstance(new CacheObserverAdapter() {
         public void afterHavingCompacted() {
           synchronized (diskRegion) {
             diskRegion.notify();
             hasBeenRolled = true;
             Log.getLogWriter().info("inside afterHavingCompacted");
           }
         }
       });

       ((LocalRegion)diskRegion).getDiskStore().forceCompaction();
       
       synchronized(diskRegion) {
         if (!hasBeenRolled) {
           try {
             diskRegion.wait();
           }
           catch (InterruptedException ex) {
             throw new util.TestException(TestHelper.getStackTrace(ex));
           }
         }
       }
       
       Log.getLogWriter().info("Data rolled into htree");
       
       getKeyInt = 0;
       keyInt = (int) DiskBB.getBB().getSharedCounters().read(DiskBB.NUM_GET);       
       do{
         getKeyInt++;
         keyInt++;
         if(keyInt> populateKeyInt){
           keyInt = 1;
         }
         key = ObjectHelper.createName(keyInt);        
         
         val = diskRegion.get(key);//for persist only mode the value will be returned from vm
         
         got = ( (LocalRegion)diskRegion ).getValueOnDiskOrBuffer(key);
         
         //values will be taken from oplogs
         if(got.toString().indexOf(DEFAULT_UPDATE_STRING) == -1){
           throw new util.TestException ("Value obtained from htree is "+got.toString()+ " whereas it is expected to be "+val.toString());
         }
         
         DiskBB.getBB().incrementCounter("DiskBB.NUM_GET", DiskBB.NUM_GET);
         
        } while(getKeyInt < ( queueSize / 2) );
      
    }
    catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of getEntries
  
  /**
   * Validate the results. Get the value from disk if it is present. Compare it
   * with the last modified value.
   */
 /* protected void validate(int index, Object got)
  {
    try {
      Object key = ObjectHelper.createName(index);
      
      // Log.getLogWriter().info("validating object...");
      if(got == null){
        // Log.getLogWriter().info("retrived null value from region...");
      } else if(al.contains(key)){
        // Log.getLogWriter().info("validating modified entry...");
        if(got.toString().indexOf(this.DEFAULT_UPDATE_STRING) == -1){
          throw new util.TestException("Validation Error. Expected updated entry. Got old entry.");
        }
      }else{        
        ObjectHelper.validate(index, got);
        // Log.getLogWriter().info("object validated...");
      }      
    }
    catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }*/

  /**
   * Populates region if the test configuration requires.
   */
  static Object populateCacheLock = "populate";
  static volatile int populateKeyInt = 0;
  protected void populateRegion()
  {
    // /add entries into the region for a particular amount of time.
    // note down how many entries are added into region
    try {
      long limit = TestConfig.tab().longAt(MapPrms.timeForPopulateCache);
      int maxKeys = MapPrms.getMaxPositiveKeys();    // defaults to 100,000 if not provided
      Object key = null, val = null, returnObj = null;
      
      Log.getLogWriter().info("populating disk region...");
      
      long startTime = System.currentTimeMillis();
      int regionSize = diskRegion.size();
      do {        
        // //this synchronization would ensure that the thread will put an
        // unique key inside the region
        synchronized (populateCacheLock) {          
          populateKeyInt++;
          key = ObjectHelper.createName(populateKeyInt);
          String objectType = MapPrms.getObjectType();
          val = ObjectHelper.createObject(objectType, populateKeyInt);
        }

        diskRegion.put(key, val);       
        regionSize = diskRegion.size();

      } while (((System.currentTimeMillis() - startTime) < limit) && (regionSize < maxKeys));

    }
    catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException may occur in concurrent environment - continuing with test");
      Log.getLogWriter().info("recovering region");
      recoverRegion();
    }
    catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }

  }// end of populateRegion

  protected synchronized void recoverRegion(){
    try{
      if (diskRegion == null || diskRegion.isDestroyed())
      {
        diskRegion = cache.createVMRegion(GemFireCachePrms.getRegionName(), attr);
        Log.getLogWriter().info("created region: "+diskRegion);
      }
    } catch (RegionExistsException rex){
      Log.getLogWriter().info("RegionExistsException can occur in concurrent environment - continuing with test");
    } catch(Exception ex){
      throw new TestException (TestHelper.getStackTrace(ex));
    }
  }//end of recoverRegion
  
  public static synchronized void HydraTask_closeTask() {
    //1. validate the states of the regions
    testInstance.closeCache();
  }//end of HydraTask_closeTask
  
  protected synchronized void closeCache(){
    try{
      if(cache != null || !cache.isClosed()){
       CacheHelper.closeCache();
      }
      
    }catch(Exception ex){
      throw new TestException("Exception while closing the cache: "+TestHelper.getStackTrace(ex));
    }
  }//end of closeCache
  
}// end of DiskRegMiscTest
