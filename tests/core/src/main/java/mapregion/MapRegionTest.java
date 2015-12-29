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
 * MapRegionTest.java
 *
 * Created on September 19, 2005, 12:45 PM
 */

package mapregion;

/**
 * The aim of this test is to validate that all region and map operations work as expected in designed circumstances
 * with all possible combinations of region attributes, eviction attributes, writers, listners, etc.
 *  
 * @author  prafulla
 * @since 5.0
 */
import hydra.*;
import util.*;
import objects.*;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;

import diskReg.DiskRegPrms;

import java.util.*;

public class MapRegionTest
{

  /*
   *Following are the names used for the region and map operations
   */
  static protected final int PUT = 1;
  static protected final int PUTALL = 2;
  static protected final int CLEAR = 3;
  static protected final int REMOVE = 4;
  static protected final int INVALIDATE = 5;
  static protected final int DESTROY = 6;
  static protected final int REGION_INVALIDATE = 7;
  static protected final int REGION_DESTROY = 8;
  static protected final int LOCAL_INVALIDATE = 9;
  static protected final int LOCAL_DESTROY = 10;
  static protected final int LOCAL_REGION_INVALIDATE = 11;
  static protected final int LOCAL_REGION_DESTROY = 12;
  static protected final int SIZE = 13;
  static protected final int ISEMPTY = 14;
  static protected final int CONTAINSVALUE = 15;  
  static protected final int TRANSACTIONS = 16;
  static protected final int LOCAL_CLEAR = 17;
  static protected final int PUT_PREVIOUS = 18;
  static protected final int FORCE_ROLLING = 19;
  static protected final int WRITE_TO_DISK = 20;
  static protected final int PUT_IF_ABSENT = 21;
  static protected final int CM_REMOVE = 22; // concurrentMap remove
  static protected final int REPLACE = 23;
  static protected final int MIRROR_TYPE_AND_OPERATION_MISMATCH = 99;

  protected long minTaskGranularitySec;   //the task granularity in seconds
  protected long minTaskGranularityMS;    //the task granularity in milliseconds

  static protected MapRegionTest mapRegionTest;
  static protected boolean isCarefulValidation = false;  
  
  static protected Cache cache;
  static protected volatile Region region;
  static protected RegionAttributes attr;
  static private String regionName;
  static boolean isReplicate = false;
  
  static private int endTestOnNumKeysInRegion = -1;
  
  //Value of EventPrms.lowerThreshold
   protected int lowerThreshold;

   //Value of EventPrms.upperThreshold
  protected int upperThreshold;

  /** Creates a new instance of MapRegionTest */
  ////////////// constructor //////////////
  public MapRegionTest() {
  }

  ////////////// hydra init task methods//////////////
  public synchronized static void HydraTask_initialize()
  {
    if (mapRegionTest == null) {
      mapRegionTest = new MapRegionTest();
      mapRegionTest.initialize();
    }
  }//end of HydraTask_initialize

  protected void initialize() {
    try {
      ////initialize cache      
      initCache();      
      attr = RegionHelper.getRegionAttributes(ConfigPrms.getRegionConfig());
      regionName = RegionHelper.getRegionDescription(ConfigPrms.getRegionConfig()).getRegionName();
  
      ///create region...     
      if (MapRegionTest.region == null) {
        region = RegionHelper.createRegion(regionName, attr);
        
        boolean tempReplicate = false, tempPersistReplicate = false;
        tempReplicate = region.getAttributes().getDataPolicy().isReplicate();
        tempPersistReplicate = region.getAttributes().getDataPolicy().isPersistentReplicate();
        
        if(tempReplicate || tempPersistReplicate) {
        	isReplicate = true;
        }
      }      
      
      minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
      minTaskGranularityMS = minTaskGranularitySec* TestHelper.SEC_MILLI_FACTOR;
      ////this parameter @link serialExecution decides for careful validations of the region methods e.g. put
      isCarefulValidation = TestConfig.tab().booleanAt(Prms.serialExecution);
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of initialize

  /**
   * Connects to DistributedSystem and creates cache       
   */

  private synchronized void initCache()
  {
    try {      
    	cache = CacheHelper.createCache(ConfigPrms.getCacheConfig());      
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of initCache
  
  public void createRegion () {
    
    try {      
     region = cache.createRegion(regionName, attr);
    } catch (RegionExistsException reex){
      Log.getLogWriter().info("RegionExistsException...may occur in concurrent environment. Continuing with test.");
    } catch (DiskAccessException dae) {
      Throwable cause = dae.getCause();	
      if (cause instanceof RegionDestroyedException)
    	Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      else throw new TestException(TestHelper.getStackTrace(dae));	
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }    
  }//end of createRegion
  
  ////////////// hydra close task methods//////////////
  public synchronized static void HydraTask_closetask() {
    if (mapRegionTest != null){
      mapRegionTest.closeTask();
      mapRegionTest = null;
    }    
  }//end of HydraTask_closetask

  protected void closeTask() {    
      MapBB.getBB().print();
      closeCache();      
  }// end of closeTask

  public void closeCache() {
    try {             
        CacheHelper.closeCache();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of closeCache
  
  ////////////// hydra end task //////////////
  /*
   * Currently this just prints the number of times the region operations got executed
   */

  public synchronized static void HydraTask_endtask() {
    if (mapRegionTest == null) {
      mapRegionTest = new MapRegionTest();
      mapRegionTest.closeTask();
      mapRegionTest = null;
    }
  }// end of HydraTask_endtask

  // ========================================================================
  // hydra task methods
  ////////////// perform according to the values which the thread has brought from conf files  //////////////
  
  /**
   * Entry operations. A thread would bring a name of entry operation from test configuration file.
   * A thread would also bring some of the parameters for that operation and would perform the operation on region.
   * 
   */

  public static void HydraTask_performEntryOperations() {
	endTestOnNumKeysInRegion = TestConfig.tab().intAt(DiskRegPrms.endTestOnNumKeysInRegion, -1);  
    mapRegionTest.performEntryOperations();
    try{
      if (endTestOnNumKeysInRegion > -1) {       
        int numKeys = mapRegionTest.region.keySet().size();
        Log.getLogWriter().info("Current numKeys is " + numKeys + ", endTestOnNumKeysInRegion is " +
          endTestOnNumKeysInRegion);
        if (numKeys >= endTestOnNumKeysInRegion) {
          throw new StopSchedulingTaskOnClientOrder("Workload based test has " + numKeys + 
             " keys in region, endTestOnNumKeysInRegion is " + endTestOnNumKeysInRegion);
        }
      }      
    } catch (CancelException cclex){
      Log.getLogWriter().info("CancelException may occur in concurrent environment - continuing with test: + cclex");
    } catch(RegionDestroyedException rgdex) {
      Log.getLogWriter().info("RegionDestroyedException may occur in concurrent environment - continuing with test");	
    }
  }

  protected void performEntryOperations() {
      
    long startTime = System.currentTimeMillis();
    lowerThreshold = TestConfig.tab().intAt(MapPrms.lowerThreshold, -1);
    upperThreshold = TestConfig.tab().intAt(MapPrms.upperThreshold, Integer.MAX_VALUE);
        
    Log.getLogWriter().info("lowerThreshold " + lowerThreshold + ", " + "upperThreshold " + upperThreshold );

    do {
      int whichOp = getEntryOperation(MapPrms.entryOperationName, isReplicate);
      int size = 0;
     
      try {
         size = region.size();
      } catch (RegionDestroyedException rdex) {
        Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent execution mode. Continuing with test.");
        recoverRegion();
      } 

      
      if (size >= upperThreshold) {
        whichOp = getEntryOperation(MapPrms.upperThresholdOperations, isReplicate);
      } else if (size <= lowerThreshold) {
        whichOp = getEntryOperation(MapPrms.lowerThresholdOperations, isReplicate);
      }

      try {
      switch (whichOp) {
      case PUT:
        putObject();
        break;
      case PUT_PREVIOUS:
        putPrevious();
        break;
      case PUTALL:
        putAllObjects();
        break;
      case REMOVE:
        removeObject();
        break;
      case INVALIDATE:
        invalidateObject();
        break;
      case DESTROY:
        destroyObject();
        break;
      case LOCAL_INVALIDATE:
        invalidateLocalObject();
        break;
      case LOCAL_DESTROY:
        destroyLocalObject();
        break;
          case PUT_IF_ABSENT:
            putIfAbsent();
            break;
          case CM_REMOVE:
            concurrentMapRemove();
            break;
          case REPLACE:
            replace();
            break;
      case SIZE:
        sizeOfRegion();
        break;
      case ISEMPTY:
        isEmptyMethod();
        break;
      case CONTAINSVALUE:
        containsValueMethod();
        break;
      case TRANSACTIONS:
        useTransactions();
        break;
      case MIRROR_TYPE_AND_OPERATION_MISMATCH:
        logMirrorTypeAndOperationMismatch();
        break;
      default: {
        throw new TestException("Unknown operation " + whichOp);
      }
        }  // switch 
      } catch (RegionDestroyedException rdex) {
        Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion();
      } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
    } while ((System.currentTimeMillis() - startTime) < minTaskGranularityMS);
  }// end of performEntryOperations

  public static void HydraTask_performRegionOperations() {
    mapRegionTest.performRegionOperations();
  }

  protected void performRegionOperations() {
    try {   
      int whichOp = getRegionOperation(MapPrms.regionOperationName, isReplicate);
      
      switch (whichOp) {
      case CLEAR:
        clearRegion();
        break;
      case REGION_INVALIDATE:
        invalidateRegion();
        break;
      case REGION_DESTROY:
        destroyRegion();
        recoverRegion();
        break;
      case LOCAL_REGION_INVALIDATE:
        invalidateLocalRegion();
        break;
      case LOCAL_REGION_DESTROY:
        destroyLocalRegion();
        break;
      case LOCAL_CLEAR:
        localClearRegion();
        break;
      case MIRROR_TYPE_AND_OPERATION_MISMATCH:
        logMirrorTypeAndOperationMismatch();
        break;
      case FORCE_ROLLING:
        forceRollOplogs();
        break;        
      case WRITE_TO_DISK:
          writeToDisk();
          break;        
      default: {
        throw new TestException("Unknown operation " + whichOp);
      }
      }
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of performRegionOperations  
  
  protected synchronized void recoverRegion() {
    try {
      if (MapRegionTest.region == null || MapRegionTest.region.isDestroyed()) {
        ////this is dirty fix...awaiting a feedback from fabricdev here on what to do?
        //// darrel said it's a bug. Logged as BUG #34269
        Log.getLogWriter().info("recovering region...");
        Thread.sleep(2 * 1000);
        createRegion();
        Log.getLogWriter().info("region recovered...");
      }
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of checkRegion

  protected int getEntryOperation(Long whichPrm, boolean disallowLocalEntryOps) {
    long limit = 60000;
    long startTime = System.currentTimeMillis();
    int op = 0;
    String operation = TestConfig.tab().stringAt(whichPrm);
    
    if (operation.equals("put"))
      op = PUT;
    else if (operation.equals("putPrevious"))
      op = PUT_PREVIOUS;
    else if (operation.equals("putAll"))
      op = PUTALL;
    else if (operation.equals("remove"))
      op = REMOVE;
    else if (operation.equals("invalidate"))
      op = INVALIDATE;
    else if (operation.equals("destroy"))
      op = DESTROY;
    else if (operation.equals("localInvalidate"))
      op = LOCAL_INVALIDATE;
    else if (operation.equals("localDestroy"))
      op = LOCAL_DESTROY;
    else if (operation.equalsIgnoreCase("putIfAbsent"))
      op = PUT_IF_ABSENT;
    else if (operation.equalsIgnoreCase("cmRemove"))
      op = CM_REMOVE;
    else if (operation.equalsIgnoreCase("replace"))
      op = REPLACE;
    else if (operation.equals("size"))
      op = SIZE;
    else if (operation.equals("isEmpty"))
      op = ISEMPTY;
    else if (operation.equals("containsValue"))
      op = CONTAINSVALUE;
    else if (operation.equals("useTransactions"))
      op = TRANSACTIONS;
    else
      throw new TestException("Unknown entry operation: " + operation);
    if (System.currentTimeMillis() - startTime > limit) {
      // could not find a suitable operation in the time limit; there may be
      // none available
      throw new TestException("Could not find an operation in " + limit
          + " millis; disallowLocalEntryOps is " + true
          + "; check that the operations list has allowable choices");
    }

    if (disallowLocalEntryOps && ((op == LOCAL_INVALIDATE) || (op == LOCAL_DESTROY))) {
      op = MIRROR_TYPE_AND_OPERATION_MISMATCH;
    }
    
    Log.getLogWriter().info("performing "+ operation +" on region " + region);
    
    return op;
  }// end of getOperation

  protected int getRegionOperation(Long whichPrm, boolean disallowLocalRegionOps) {
    int op = 0;
    String operation = TestConfig.tab().stringAt(whichPrm);
    
    if (operation.equals("clear"))
      op = CLEAR;
    else if (operation.equals("regionInvalidate"))
      op = REGION_INVALIDATE;
    else if (operation.equals("regionDestroy"))
      op = REGION_DESTROY;
    else if (operation.equals("localRegionInvalidate"))
      op = LOCAL_REGION_INVALIDATE;
    else if (operation.equals("localRegionDestroy"))
      op = LOCAL_REGION_DESTROY;
    else if (operation.equals("localClear"))
      op = LOCAL_CLEAR;
    else if (operation.equals("forceRolling"))    
      op = FORCE_ROLLING;
    else if (operation.equals("writeToDisk"))    
      op = WRITE_TO_DISK;
    else
      throw new TestException("Unknown region operation: " + operation);

    if (disallowLocalRegionOps && ((op == LOCAL_REGION_INVALIDATE) || (op == LOCAL_CLEAR))) {
      op = MIRROR_TYPE_AND_OPERATION_MISMATCH;
    }
    
    Log.getLogWriter().info("performing "+ operation +" on region" + region);

    return op;
  }// end of getRegionOperation

  ////////////// region methods //////////////
  /*
   *In every operation check if the region is destroyed...if yes then create the region
   *
   */

  /**
   * putKeyInt is the number of keys on which puts have done 
   * so far in a vm on a region.
   */
  protected void putObject() {
    try {      
      Object key=null, val=null;
      String objectType = MapPrms.getObjectType();      
      
      int putKeyInt = (int) MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_PUT);
            key = ObjectHelper.createName(putKeyInt);            
            val = ObjectHelper.createObject(objectType, putKeyInt);
          
          Object got = region.get(key);              			
      Log.getLogWriter().info("executing put(" + key + ", " + val + ")");
          Object returnObj = region.put(key, val);                   
      Log.getLogWriter().info("done with put(" + key + ", " + val + ")");

          ////verify if the correct object is returned or not.        			
          ////verify for region sizes
          if (isCarefulValidation) {
            if (got == null) {
              Log.getLogWriter().info("No value associated with the key: " + key+ " before put operation");
              if (returnObj != null)
                 throw new TestException("Expected null to be returned from put, but it is " + returnObj);
        } else if (!PartitionRegionHelper.isPartitionedRegion(region)) {
              if (got.equals(returnObj)) {
                Log.getLogWriter().info("Correct value returned on region.put(key, value)");
          } else {  
                throw new TestException("INCORRECT value returned on region.put(key, value)");
              }
            }

            Object gotAfterPut = region.get(key);
            if (!gotAfterPut.equals(val)) {
              throw new TestException("Validation error after put operation. Expected: " + val+ " got: " + gotAfterPut);
            }
          }
    } catch (RegionDestroyedException rdex) {
        Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent execution mode. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of putObject

  /** Put a key that was previously used in this test */
  protected void putPrevious() {
    try {      
      int currentKey = (int)MapBB.getBB().getSharedCounters().read(MapBB.NUM_PUT);
      String objectType = MapPrms.getObjectType();      
      
          int putKeyInt = TestConfig.tab().getRandGen().nextInt(Math.max(currentKey-1, currentKey-50), currentKey-1);
          Object key = ObjectHelper.createName(putKeyInt);            
          Object val = ObjectHelper.createObject(objectType, TestConfig.tab().getRandGen().nextInt());
          Object got = region.get(key);              			
      Log.getLogWriter().info("Putting with previous key " + key + ", value " + val + ", old value is " + got);
      Log.getLogWriter().info("executing put(" + key + ", " + val + ")");
          Object returnObj = region.put(key, val);                   
      Log.getLogWriter().info("done with put(" + key + ", " + val + ")");
      Log.getLogWriter().info("Done putting with previous key " + key + ", value " + val + " returned value is " + returnObj);

          ////verify for region sizes
          if (isCarefulValidation) {
            if (got == null) {
              Log.getLogWriter().info("No value associated with the key: " + key+ " before put operation");
              if (returnObj != null)
                 throw new TestException("Expected null to be returned from put, but it is " + returnObj);
        } else if (!PartitionRegionHelper.isPartitionedRegion(region)) {
              if (got.equals(returnObj)) {
                Log.getLogWriter().info("Correct value returned on region.put(key, value)");
          } else { 
            throw new TestException("INCORRECT value " + returnObj + " returned on region.put(" + key + ", " + val + "); expected " + got); 
              }
              }

            Object gotAfterPut = region.get(key);
            if (!gotAfterPut.equals(val)) {
              throw new TestException("Validation error after put operation. Expected: " + val+ " got: " + gotAfterPut);
            }
          }
    } catch (RegionDestroyedException rdex) {
        Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent execution mode. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of putObject

  protected void putAllObjects() {
    Object key=null, val=null;
    String objectType = MapPrms.getObjectType();
    Map m = new TreeMap();   
    int mapSize = 0;
    int numEntries = TestConfig.tab().getRandGen().nextInt(1, 25);
    
    int putAllKeyInt;
    do {
      //// MapBB.NUM_PUT is taken because it will give the actual number of keys already put inside region.
      putAllKeyInt = (int) MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_PUT);
      key = ObjectHelper.createName(putAllKeyInt);        
      val = ObjectHelper.createObject(objectType, putAllKeyInt);
      m.put(key, val);
      mapSize++;
    } while (mapSize < numEntries);

    try {

      Log.getLogWriter().info("executing putAll of " + m.toString());
      region.putAll(m);      
      Log.getLogWriter().info("done with putAll");

      if (isCarefulValidation) {
        key = null; 
        Object got = null;
        int i = putAllKeyInt;
        while (i < (putAllKeyInt - mapSize)) {
          key = ObjectHelper.createName(i);
          got = region.get(key);
          if (got == null) {
            throw new TestException("get on key: " + key+ " is returning null value after putAll");
          }
          i++;
        }
      }
      Log.getLogWriter().info("----performed putAll operation on region");
    } catch (RegionDestroyedException rdex) {
        Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment mode. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of putAllObject

  protected void removeObject() {
    try {     

      Object key=null;
      int removeKeyInt = (int) MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_REMOVE);
          key = ObjectHelper.createName(removeKeyInt);
        
        Object got = region.get(key);
        if (got == null) {
          //Log.getLogWriter().info("Key: " + key + " does NOT exist in region");
      } else {
        Log.getLogWriter().info("executing Map remove(" + key + ")");
          Object returnObj = region.remove(key);
        Log.getLogWriter().info("done with Map remove(" + key + ")");
          
          ////verify if the correct object is returned or not. 
          if (isCarefulValidation) {
          if (!PartitionRegionHelper.isPartitionedRegion(region)) {
            if (got.equals(returnObj)) {
              Log.getLogWriter().info("Correct value returned on region.remove(key)");
            } else {                  					
              throw new TestException("INCORRECT value returned on region.remove(key)");
            }            
          }
            //// size check is not important because remove operation is internally calling region.destroy(key).            
          }         
        }
    } catch (RegionDestroyedException rdex) {
        Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {      
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of removeObject

  protected void invalidateObject() { 
    try {      
      
      Object key=null;      
      int invalidateKeyInt = (int) MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_INVALIDATE);
      
          key = ObjectHelper.createName(invalidateKeyInt);
        Object got = region.get(key);

        if (got == null) {
        //Log.getLogWriter().info("NO entry exists in region " + region.getName() + " for key " + key);
      } else {
        Log.getLogWriter().info("executing invalidate(" + key + ")");
          region.invalidate(key);
        Log.getLogWriter().info("done with invalidate(" + key + ")");

          Object gotAfterInvalidate = region.get(key);          
          ////verify if the correctness of invalidate operation 
          if (isCarefulValidation) {
            if (gotAfterInvalidate != null) {
              throw new TestException("region.get(Object key) returns NON NULL value immediately after region.invalidate(Object key)"+ gotAfterInvalidate);
            }
          }
        }
    } catch (EntryNotFoundException enfe) {      
        Log.getLogWriter().info("EntryNotFoundException ...may occur in concurrent execution. Continuing with test.");      
    } catch (RegionDestroyedException rdex) {
        Log.getLogWriter().info("RegionDestroyedException...can occur in concurrent environment. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of invalidateObject

  protected void destroyObject() {   
    try {
      
      Object key=null;
      
      int destroyKeyInt = (int) MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_DESTROY);
      key = getExistingKey(region);
      if (key == null) {
        Log.getLogWriter().info("No keys in region, returning from destroyObject()");
        return;
      }

        Object got = region.get(key);

          int sizeBeforeDestroy = region.size();
      Log.getLogWriter().info("executing destroy(" + key + ")");
          region.destroy(key);
      Log.getLogWriter().info("done with destroy(" + key + ")");

          int sizeAfterDestroy = region.size();
          Object gotAfterDestroy = region.get(key);
          
          ////verify if the correctness of destroy operation
          if (isCarefulValidation) {
            if (sizeAfterDestroy == (sizeBeforeDestroy - 1)) {
              Log.getLogWriter().info("sizeAfterDestroy is equal to (sizeBeforeDestroy-1)");              
        } else {  
          throw new TestException("sizeAfterDestroy=" + sizeAfterDestroy + " is NOT equal to (sizeBeforeDestroy-1)=" + (sizeBeforeDestroy-1));
            }
            
            Log.getLogWriter().info("gotAfterDestroy " + gotAfterDestroy);
            if (gotAfterDestroy != null) {                
              throw new TestException("region.get(Object key) returns non null value immediately after region.destroy(Object key) "+ gotAfterDestroy + " for region: " + region.getName());
            }
          }
    } catch (EntryNotFoundException enfe) {      
        Log.getLogWriter().info("EntryNotFoundException ...may occur in concurrent execution. Continuing with test.");      
    } catch (RegionDestroyedException rdex) {      
        Log.getLogWriter().info("RegionDestroyedException...can occur in concurrent environment. Continuing with test.");
        recoverRegion();
    } catch (Exception ex) {
        throw new TestException(TestHelper.getStackTrace(ex));
        }
  }//end of destroyObject

  /** 
   * ConcurrentMap methods
   **/ 
  protected void putIfAbsent() {
    int keyBase;
    Object existingKey;
    Object key=null, val=null;
    String objectType = MapPrms.getObjectType();      

    // If we select a new key (for successful putIfAbsent), then expectSuccess is true
    // When we use an existing key, we cannot set our expectations (since entry may be destroyed, etc) 
    boolean expectSuccess = false;
    boolean invalidatedEntry = false;
      
    if (TestConfig.tab().getRandGen().nextBoolean()) {
      keyBase = (int)MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_PUT);
      key = ObjectHelper.createName(keyBase);            
      val = ObjectHelper.createObject(objectType, keyBase);
      expectSuccess = true;
      Log.getLogWriter().info("executing putIfAbsent(" + key + ", " + val + ") with expectSuccess = " + expectSuccess);
    } else { // use an existing entry if one exists
      existingKey = getExistingKey(region);

      // If there are no entries in the region, use a new key and expect success
      if (existingKey == null) {  
        expectSuccess = true;
        keyBase = (int)MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_PUT);
        key = ObjectHelper.createName(keyBase);            
        val = ObjectHelper.createObject(objectType, keyBase);
      } else {  // the key exists, expect failure
        key = existingKey;
        val = ObjectHelper.createObject(objectType, TestConfig.tab().getRandGen().nextInt());
    }
      Log.getLogWriter().info("executing putIfAbsent(" + key + ", " + val + ") with expectSuccess = " + expectSuccess);
    }

    Object retVal = null;
    Object oldVal = null;
    try {      
      oldVal = region.get(key);              			
      invalidatedEntry = false;
      if (oldVal == null && region.containsKey(key)) {
        invalidatedEntry = true;
      }

      retVal = region.putIfAbsent(key, val);                   
      Log.getLogWriter().info("putIfAbsent returned " + retVal);
 
      if (isCarefulValidation) {
        if (expectSuccess) {
          if (retVal != null) {
            Log.getLogWriter().info("putIfAbsent was expected to succeed, but non-null value " + retVal + " returned");
            throw new TestException("putIfAbsent was expected to succeed, but non-null value " + retVal + " returned" + TestHelper.getStackTrace());
          } 
   
          Object valAfterPut = region.get(key);
          if (!valAfterPut.equals(val)) {
            Log.getLogWriter().info("get after putIfAbsent of " + val + " returned " + valAfterPut);
            throw new TestException("get after putIfAbsent of " + val + " returned " + valAfterPut + TestHelper.getStackTrace());
          }
        } else if (!PartitionRegionHelper.isPartitionedRegion(region)) {  // expectSuccess = false
          if (oldVal != null) {
            if (oldVal.equals(retVal)) {
            Log.getLogWriter().info("Correct (previous) value returned on region.putIfAbsent(key, value)");
            } else {              
              throw new TestException("INCORRECT value " + retVal + " returned on region.putIfAbsent(" + key + ", " + val + ")");
            }
          }
        }
      }  // end of carefulValidation
    } catch (RegionDestroyedException rdex) {
        Log.getLogWriter().info("RegionDestroyedException may occur in both serial and concurrent execution mode. Continuing with test.");
      recoverRegion();
     // go to next iteration of loop to start creating more entries
     } catch (Exception ex) {
         throw new TestException(TestHelper.getStackTrace(ex));
    }
  } //end of putIfAbsent
 
  protected void concurrentMapRemove() {
    Object key = null;
    Object oldVal = null;
    boolean expectSuccess = true;
    boolean removed = false;
    String objectType = MapPrms.getObjectType();      
      
    int keyBase = (int) MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_REMOVE);
    int keysPut = (int) MapBB.getBB().getSharedCounters().read(MapBB.NUM_PUT);
    // make sure we have more keys to remove (based on total keys put into region so far)
    if (keyBase >= keysPut) {
      Log.getLogWriter().info("early return from remove, need new keys to work on");
      return;
    } 

    key = ObjectHelper.createName(keyBase);
      
    try {     
      // pass in a different oldVal (sometimes) to cause remove to fail
      if (TestConfig.tab().getRandGen().nextBoolean()) {
         if (region.containsKey(key)) {
            oldVal = region.get(key);  // success
         } else {
            // key doesn't exist, expect remove to fail
            expectSuccess = false;
         }
      } else {
         expectSuccess = false;
         oldVal = ObjectHelper.createObject(objectType, TestConfig.tab().getRandGen().nextInt());
         Log.getLogWriter().info("substituting " + oldVal + " for existing value " + region.get(key) + " to test condition value.  expectSuccess = " + expectSuccess);
      }
      Log.getLogWriter().info("executing concurrentMap remove(" + key + ", " + oldVal + ") expectSuccess = " + expectSuccess);
      removed = region.remove(key, oldVal);
      Log.getLogWriter().info("concurrentMap remove() returned " + removed);
    } catch (RegionDestroyedException rdex) {
        Log.getLogWriter().info("concurrentMap remove caught RegionDestroyedException; may occur in both serial and concurrent environment. Continuing with test.");
        recoverRegion();
        return;
    } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
        if (isCarefulValidation) {
          throw new TestException(TestHelper.getStackTrace(e));
        } else {
          Log.getLogWriter().info("Caught RegionDestroyedException; may occur in concurrent environment. Continuing with test.");
          return;
        }
    } catch (Exception ex) {      
      throw new TestException(TestHelper.getStackTrace(ex));
    }
          
    if (isCarefulValidation) {
       if (expectSuccess) {
          if (!removed) throw new TestException(TestHelper.getStackTrace());
       } else {  // expect failure
          if (removed) throw new TestException(TestHelper.getStackTrace());
       }
    }  // careful validation
  }  //end of concurrentMapRemove

  /** ConcurrentMap testing: region.replace()  */
  protected void replace() {
    String objectType = MapPrms.getObjectType();      
    Object key;
    Object oldVal, newVal = null;
    boolean expectSuccess = true;
    boolean replaced = false;
    
    key = getExistingKey(region);
    if (key == null) {
      Log.getLogWriter().info("early return from replace, getExistingKey returned null");
      return; 
    }

    oldVal = region.get(key);
    newVal = ObjectHelper.createObject(objectType, TestConfig.tab().getRandGen().nextInt());

    try {      
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use api with oldVal
         if ((oldVal != null) && TestConfig.tab().getRandGen().nextBoolean()) { // force replace failures (replace condition value with the newVal)
            expectSuccess = false;
            Log.getLogWriter().info("executing replace(" + key + ", " + newVal + ", " + oldVal + "), expectSuccess = " + expectSuccess);
            replaced = region.replace(key, newVal, oldVal);
            Log.getLogWriter().info("replace returned " + replaced);
         } else {
            Log.getLogWriter().info("executing replace(" + key + ", " + oldVal + ", " + newVal + "), expectSuccess = " + expectSuccess);
            replaced = region.replace(key, oldVal, newVal);
            Log.getLogWriter().info("replace returned " + replaced);
         }
      } else { // use api without oldVal
         Log.getLogWriter().info("executing replace(" + key + ", " + newVal + "), with oldVal " + oldVal + " expectSuccess = " + expectSuccess);
         Object retVal = region.replace(key, newVal);
         // Determine if replace occurred (replaced will only be tested in the serial/carefulValidation case, so do not consider the concurrent case here
         // replace returns the oldVal if key is mapped to a value, otherwise, it returns null
         // However, nulls are allowed with this API, to retVal = oldVal = null can also indicate a successful replace
         replaced = false;
         if (oldVal == null) {
           if (retVal == null) {
             replaced = true;
           } 
         } else { // non-null oldVal
           if ((retVal != null) && retVal.equals(oldVal)) {
             replaced = true;
           } 
         }
         Log.getLogWriter().info("replace returned " + retVal + " replaced = " + replaced);
      }
    } catch (RegionDestroyedException rdex) {
        Log.getLogWriter().info("Caught RegionDestroyedException; may occur in both serial and concurrent execution mode. Continuing with test.");
        recoverRegion();
        // returning (so more entries can be created via other ops
        return;
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }

    if (isCarefulValidation) {
      Object gotAfterReplace = region.get(key);
      if (expectSuccess) {
        if (!replaced) {
          throw new TestException(TestHelper.getStackTrace());
        }
        if (!gotAfterReplace.equals(newVal)) {
          throw new TestException("Validation error after replace operation. Expected: " + newVal + " got: " + gotAfterReplace);
        }
      } else {  // expect failure
        if (replaced) {
          throw new TestException(TestHelper.getStackTrace());
        }
      }
    }  // careful validation
  }  //end of concurrentMapRemove

  /** Return a random key currently in the given region.
   *
   *  @param aRegion The region to use for getting an existing key 
   *
   *  @returns A key in the region.
   */
  protected Object getExistingKey(Region aRegion) {
     Object key = null;
     Object[] keyList = aRegion.keySet().toArray();
     int index = TestConfig.tab().getRandGen().nextInt(0, keyList.length-1);
     if (index > 0) {
        key = keyList[index];
     }
     return key;
  }

  protected void clearRegion() {
    try {

      region.clear();

      int sizeAfterClear = region.size();

      //// verify if the correctness of clear operation
      if (isCarefulValidation) {
        if (sizeAfterClear == 0) {
          Log.getLogWriter().info("sizeAfterClear is equal to ZERO");
        } else {               		
          throw new TestException("sizeAfterClear is NOT equal ZERO. Region holds few entries even after region.clear()");
        }

        boolean isEmpty = region.isEmpty();
           				
        if (isEmpty) {
          Log.getLogWriter().info("region after clear operation is empty");
        } else {
          throw new TestException("region after clear operation is NOT empty");
        }
      }
      MapBB.incrementCounter("MapBB.NUM_CLEAR", MapBB.NUM_CLEAR);
    } catch (RegionDestroyedException rdex) {
        Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of clearRegion
  
  protected void localClearRegion() {

    try {
      region.localClear();
      int sizeAfterLocalClear = region.size();

      //// verify if the correctness of clear operation
      if (isCarefulValidation) {
        if (sizeAfterLocalClear == 0) {
          Log.getLogWriter().info("sizeAfterLocalClear is equal to ZERO");
        } else {                          
          throw new TestException("sizeAfterClear is NOT equal ZERO. Region holds few entries even after region.clear()");
        }

        boolean isEmpty = region.isEmpty();
                                        
        if (isEmpty) {
          Log.getLogWriter().info("region after localClear operation is empty");
        } else {
          throw new TestException("region after localClear operation is NOT empty");
        }
      }
      MapBB.incrementCounter("MapBB.NUM_LOCAL_CLEAR", MapBB.NUM_LOCAL_CLEAR);
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of localClearRegion

  protected void invalidateRegion() {
    try {
      
      region.invalidateRegion();

      if (isCarefulValidation) {
        Collection values = region.values();
        Iterator itr = values.iterator();
        while (itr.hasNext()){
          Object got  = itr.next();
          if (got != null) {
            throw new TestException("region.get(Object key) returns non null value immediately after region.invalidateRegion");
          }
        }          
      }
      MapBB.incrementCounter("MapBB.NUM_REGION_INVALIDATE",MapBB.NUM_REGION_INVALIDATE);
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion();      
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
    
  }//end of invalidateRegion

  protected void destroyRegion() {
    try {

      region.destroyRegion();

      if (isCarefulValidation) {
        try {
          if (PartitionRegionHelper.isPartitionedRegion(region)) {
            region.destroyRegion();
          } else {
          region.clear();
        }
          throw new TestException("Should have thrown RegionDestroyedException");
        } catch (RegionDestroyedException rdex) {
          //expected
        }
      }
      MapBB.incrementCounter("MapBB.NUM_REGION_DESTROY",MapBB.NUM_REGION_DESTROY);
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of destroyRegion

  ////////// all misc methods which belong to Map //////////
  /*
   *These are the methods of region which do not alter the entries in region as such
   *But they have to be tested for the correctness of the returned values in multi vm & multi threaded environment
   */

  protected void sizeOfRegion() {
    try {

      int i = region.size();
      Log.getLogWriter().info("----size of region is " + i);
      MapBB.incrementCounter("MapBB.NUM_SIZE", MapBB.NUM_SIZE);
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of sizeOfRegion

  protected void containsValueMethod() {
    try {
      boolean containsValue;
      if (isCarefulValidation) {
        Collection values = region.values();
        Iterator itr = values.iterator();
        while (itr.hasNext()){
          Object got  = itr.next();
          if (got != null) {//not sure about invalidated values hence this check
            containsValue = region.containsValue(got);
            if(!containsValue){
              throw new TestException("region.containsValue(Object val) should retun true but got: "+containsValue);
            }
          }
        }          
      } else {
        Collection values = region.values();
        Iterator itr = values.iterator();
        while (itr.hasNext()){
          Object got  = itr.next();
          if (got != null) {//not sure about invalidated values hence this check
            containsValue = region.containsValue(got);
            if(!containsValue){
              Log.getLogWriter().info("region.containsValue(Object val) should retun true but got: "+containsValue+". May occur in concurrent execution. Continuing with test.");
            }
          }
        }          
      }
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of containsValueMethod

  protected void isEmptyMethod() {
    try {
      
      boolean val = region.isEmpty();
      MapBB.incrementCounter("MapBB.NUM_ISEMPTY", MapBB.NUM_ISEMPTY);
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of isEmptyMethod  

  ////////// all local operations //////////
  /*
   *These methods are not getting invoked on mirroredRegion
   */
  protected void invalidateLocalObject() {
    try {
      
      Object key=null;
      int invalidateKeyInt = (int) MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_INVALIDATE);
      
          key = ObjectHelper.createName(invalidateKeyInt);
        Object got = region.get(key);

        if (got == null) {
          //Log.getLogWriter().info("NO entry exists in region " + region.getName() + "for key" + key);
      } else {
          region.localInvalidate(key);

          Object gotAfterInvalidate = null;
          Region.Entry entry = region.getEntry(key);
          if (entry != null) {
             gotAfterInvalidate = entry.getValue();
          }

          ////verify if the correctness of invalidate operation 
          if (isCarefulValidation) {
            if (entry == null || gotAfterInvalidate != null) {
              throw new TestException("region.get(Object key) returns NON NULL value immediately after region.localInvalidate(Object key)"+ gotAfterInvalidate);
            }
          }
        }
    } catch (EntryNotFoundException enfe) {       // thrown from localInvalidate
        Log.getLogWriter().info("EntryNotFoundException ...may occur in concurrent execution. Continuing with test.");      
    } catch (EntryDestroyedException edex) {      // thrown from Entry.getValue()
        Log.getLogWriter().info("EntryDestroyedException ...may occur in concurrent execution. Continuing with test.");      
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...can occur in concurrent environment. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of invalidateLocalObject

  
  protected void destroyLocalObject() {
    try {
      
      ////  will use the same time as destroy operation.
      Object key=null;
      int destroyKeyInt = (int) MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_DESTROY);
      
          key = ObjectHelper.createName(destroyKeyInt);
        Object got = region.get(key);

        if (got == null) {
          //Log.getLogWriter().info("NO entry exists in region " + region.getName() + "for key " + key);
      } else {
           int sizeBeforeDestroy = region.size();
          region.localDestroy(key);
          
          int sizeAfterDestroy = region.size();
          
          ////verify if the correctness of destroy operation
          if (isCarefulValidation) {
            if (sizeAfterDestroy == (sizeBeforeDestroy - 1)) {
              Log.getLogWriter().info("sizeAfterDestroy is equal to (sizeBeforeDestroy-1)");              
          } else {
            throw new TestException("sizeAfterDestroy=" + sizeAfterDestroy + " is NOT equal to (sizeBeforeDestroy-1)=" + (sizeBeforeDestroy-1));
            }           
          }          
        }
    } catch (EntryNotFoundException enfe) {      
        Log.getLogWriter().info("EntryNotFoundException ...may occur in concurrent execution. Continuing with test.");      
    } catch (RegionDestroyedException rdex) {      
      Log.getLogWriter().info("RegionDestroyedException...can occur in concurrent environment. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of destroyLocalObject

  protected void destroyLocalRegion() {
    try {
      region.localDestroyRegion();
      if (isCarefulValidation) {
        try {
          region.clear();
          throw new TestException("Should have thrown RegionDestroyedException");
        } catch (RegionDestroyedException rdex) {
          //expected
        }
      }
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...can occur in concurrent environment. Continuing with test.");
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of destroyLocalRegion

  protected void invalidateLocalRegion() {
    try {

      region.localInvalidateRegion();

      if (isCarefulValidation) {
        Collection values = region.values();
        Iterator itr = values.iterator();
        while (itr.hasNext()){
          Object got  = itr.next();
          if (got != null) {
            throw new TestException("region.get(Object key) returns non null value immediately after region.localInvalidateRegion");
          }
        }          
      }
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of invalidateLocalRegion

  ////////////// region methods //////////////
  /*
   *These methods are to test the transactional behavior of Map and region methods
   */

  protected void useTransactions() {
    putGetPutAllDestroyTxn();
    putRemoveClearTxn();
  }//useTransactions

  protected void putGetPutAllDestroyTxn() {
    try {
      CacheTransactionManager cacheTxnMgr = cache.getCacheTransactionManager();

      cacheTxnMgr.begin();
      int i = (new Random()).nextInt();
      for (int j = 0; j < 10; j++) {
        region.put(new Integer(i), java.lang.Integer.toString(i));
        i++;
      }

      i = (new Random()).nextInt();
      Map m = new TreeMap();
      for (int j = 0; j < 10; j++) {
        m.put(new Integer(i), java.lang.Integer.toString(i));
        i++;
      }
      region.putAll(m);

      Set s = region.keySet();
      Iterator itr = s.iterator();
      if (itr.hasNext()) {
        region.destroy(itr.next());
      }
      cacheTxnMgr.commit();
      Log.getLogWriter().info("----transaction is committed");

    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//putGetPutAllDestroyTxn

  protected void putRemoveClearTxn() {
    try {
      CacheTransactionManager cacheTxnMgr = cache.getCacheTransactionManager();

      cacheTxnMgr.begin();
      int i = (new Random()).nextInt();
      for (int j = 0; j < 10; j++) {
        region.put(new Integer(i), java.lang.Integer.toString(i));
        i++;
      }

      region.clear();
      if (region.size() != 0) {
        Log.getLogWriter().info("----clear operation on region inside transaction is not successful");
      }

      i = (new Random()).nextInt();
      Map m = new TreeMap();
      for (int j = 0; j < 10; j++) {
        m.put(new Integer(i), java.lang.Integer.toString(i));
        i++;
      }
      region.putAll(m);

      Set s = region.keySet();
      Iterator itr = s.iterator();
      if (itr.hasNext()) {
        region.remove(itr.next());
      }
      cacheTxnMgr.rollback();
      Log.getLogWriter().info("----transaction is rolled back");

    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//putRemoveClearTxn

  
  //----------------------------------------------------------------------------
  //  Validation
  //----------------------------------------------------------------------------

  /**
   *  Validates that an indexed object has the right content.
   */
  protected void validate(int index, Object obj) {
    if (obj == null) {
      Log.getLogWriter().info("Validating null:" + index + "=" + obj);
    } else {
      Log.getLogWriter().info("Validating " + obj.getClass().getName() + ":" + index + "=" + obj);
    }

    ObjectHelper.validate(index, obj);

    if (obj == null) {
      Log.getLogWriter().info("Validated null:" + index + "=" + obj);
    } else {
      Log.getLogWriter().info("Validated " + obj.getClass().getName() + ":" + index + "=" + obj);
    }

  }//end of validate
  
  /**
   * Method returns the size of a region
   */
  protected int getSize() {
    int size = 0;
    try {
      size = region.size();
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
    return size;
  }//end of getSize

  /**
   * Method returns the entrySet of a region
   */
  protected Set getEntrySet() {
    Set entrySet = null;
    try {
      entrySet = region.entrySet();
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...can occur in concurrent environment. Continuing with test.");
      recoverRegion();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
    return entrySet;
  }//end of getEntrySet
  
  protected void logMirrorTypeAndOperationMismatch (){
   Log.getLogWriter().info("Mirror type and operation mismatch. Getting next operation..."); 
  }
  
  /**
   * Perform force rolling of oplogs if the region is disk region.
   *
   */
  
  protected static final int NO_DISK = 1001;
  protected static final int DISK_FOR_OVRFLW = 1002;           
  protected static final int DISK_FOR_PERSIST = 1003;        
  protected static final int DISK_FOR_OVRFLW_PERSIST = 1004; 
  
  protected boolean persistentReplicate;
  protected int evictionLimit ;
  protected int regionType = 0;
  
  protected void forceRollOplogs () {
    try {
      persistentReplicate = attr.getDataPolicy().isPersistentReplicate();
      evictionLimit = attr.getEvictionAttributes().getMaximum();    
      
      // set the type of region for this VM      
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
      
      if(regionType == DISK_FOR_PERSIST || regionType == DISK_FOR_OVRFLW_PERSIST || regionType == DISK_FOR_OVRFLW){
        region.forceRolling();
        Log.getLogWriter().info("force rolled oplogs");
      }
      
      if(regionType == NO_DISK){
        try{
          region.forceRolling();
          throw new TestException ("Should have thrown UnsupportedOperationException while performing forceRolling on non disk regions");
        } catch (UnsupportedOperationException ignore) {
          //expected
        }
      }
            
    }
    catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion();
    }
    catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of performForceRollingOfOplogs
  
  /**
   * Perform write to disk if region is configured to write to disk
   *
   */  
  
  protected void writeToDisk () {
    try {
      persistentReplicate = attr.getDataPolicy().isPersistentReplicate();
      evictionLimit = attr.getEvictionAttributes().getMaximum();    
            
      if (persistentReplicate) {
        if (evictionLimit <= 0)
          regionType = DISK_FOR_PERSIST;
      } else {
          regionType = DISK_FOR_OVRFLW_PERSIST;
      }
      
      if(regionType == DISK_FOR_PERSIST || regionType == DISK_FOR_OVRFLW_PERSIST){
        region.writeToDisk();
        Log.getLogWriter().info("writeToDisk performed on persistent region");
      }            
    }
    catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion();
    }
    catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }

}//end  of MapRegionTest
