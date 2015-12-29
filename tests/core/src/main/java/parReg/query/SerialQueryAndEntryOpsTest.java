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
package parReg.query; 

import event.*;
import java.util.*;

import objects.Portfolio;

import parReg.query.index.IndexTest;
import pdx.PdxTest;
import pdx.PdxTestVersionHelper;
import util.*;
import hydra.*;
import query.*;
import query.QueryTest;
import hydra.blackboard.*;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.types.*;
import com.gemstone.gemfire.cache.query.internal.types.*;
import com.gemstone.gemfire.cache.query.internal.*;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * A version of the <code>QueryTask</code> that performs operations
 * serially on partitioned region.  
 */
public class SerialQueryAndEntryOpsTest extends query.QueryTest {
  protected int numOfInitObjects = 30;
  protected int maxId = 100; //for allowing duplicate id
  protected int numOfQueryStrings = 9; //need to update if add more test cases
  protected static int totalNumOfVmsToStop;   
  protected static boolean serialExecution;

  /* hydra task methods */
  /* ======================================================================== */
  public synchronized static void HydraTask_initialize() {
    PRObserver.installObserverHook();
    PRObserver.initialize(RemoteTestModule.getMyVmid());
     if (queryTest == null) {
        queryTest = new SerialQueryAndEntryOpsTest();
        queryTest.initialize(); 
     }
     ParRegQueryBB.getBB().getSharedCounters().zero(ParRegQueryBB.operationCount);
     ParRegQueryBB.getBB().getSharedCounters().zero(ParRegQueryBB.stopStartVmsCounter);
     ParRegQueryBB.getBB().getSharedCounters().zero(ParRegQueryBB.stopStartSignal);
     ParRegQueryBB.getBB().getSharedCounters().zero(ParRegQueryBB.queryCount);
     
     totalNumOfVmsToStop = TestConfig.tab().intAt(query.QueryPrms.totalNumOfVmsToStop,0);
     serialExecution = TestConfig.tab().booleanAt(hydra.Prms.serialExecution, true);
  }  
  
  public void initialize() {
    super.initialize();
  }
  
  public static void HydraTask_logRegions() {
    PdxTest.initClassLoader();
    Cache aCache = CacheFactory.getAnyInstance();
    Set<Region<?, ?>> regions = aCache.rootRegions();
    for (Region aRegion: regions) {
      Log.getLogWriter().info(aRegion.getFullPath() + " is size " + aRegion.size());
      
      int activeCount = 0;
      int inactiveCount = 0;
      int neitherCount = 0;
      Log.getLogWriter().info("Looking for status");
      for (Object key: aRegion.keySet()) {
        Object value = aRegion.get(key);
        if (value instanceof PdxInstance) {
          value = PdxTestVersionHelper.toBaseObject(value);
        }
        NewPortfolio pf = (NewPortfolio)value;
        if (pf.status.equals("active")) {
          activeCount++;
        } else if (pf.status.equals("inactive")) {
          inactiveCount++;
        } else {
          Log.getLogWriter().info("neither: " + pf);
          neitherCount++;
        }
      }
      Log.getLogWriter().info("For region" + aRegion.getFullPath() + " activeCount is " +
          activeCount + ", inactiveCount is " + inactiveCount + ", neitherCount is " + neitherCount);
    }
  }
  
  public synchronized static void HydraTask_reInitialize() {
    if (queryTest == null) {
       queryTest = new SerialQueryAndEntryOpsTest();
       queryTest.initialize(); 
    }
 }  
    
  public static void HydraTask_populateRegion() {
    PdxTest.initClassLoader();
    ((SerialQueryAndEntryOpsTest)queryTest).populateRegion();
  }
  
  public static synchronized void HydraTask_doStopStartVmAndQueryOperation() {
    PdxTest.initClassLoader();
    ((SerialQueryAndEntryOpsTest)queryTest).doStopStartVmAndQueryOperation();
  }
  
  public static void HydraTask_validateCountStar() throws Throwable {
    new QueryTest().verifyCountQueryResults();
  }


  /**
   * @see #HydraTask_getEntriesFromServer
   */
  protected void populateRegion() {
    int i;
    for (i = 0; i < numOfRegions; i++) {   //populate to all the regions
      Region aRegion = CacheHelper.getCache().getRegion(REGION_NAME + ("" + i));
      populateRegion(aRegion);      
    }
    // sleep for 30 secs to allow distribution
    if (i==numOfRegions) {
      Log.getLogWriter().info("finishing populating region, sleep for 30 sec");
      MasterController.sleepForMs(30000);
    }
  }
  
  protected void populateRegion(Region aRegion) { 
    for (int i=1; i <= numOfInitObjects; i++) {
      addObject(aRegion, true, false);
    }
    // sleep for 60 secs to allow distribution
    //MasterController.sleepForMs(60000);
  }
  
  public static synchronized void HydraTask_createIndex() {
    if(queryTest == null) {
        queryTest = new SerialQueryAndEntryOpsTest();
    }
    ((SerialQueryAndEntryOpsTest)queryTest).createAllIndex();
 }
  
  protected void doEntryAndQueryOperations() {
    SharedCounters counters = ParRegQueryBB.getBB().getSharedCounters();
    counters.increment(ParRegQueryBB.operationCount);
    
    try {
    super.doEntryAndQueryOperations();
    } catch (CancelException e) {
      if (counters.incrementAndRead(ParRegQueryBB.stopStartSignal) == 1) {
        // we are in the process of recycling a VM
        if (StopStartVMs.niceKillInProgress()) {  // this VM is being stopped
           Log.getLogWriter().info("Caught CancelException -- expected in concurrent tests with stop start vm, continuing ... " + e);
        } else {
          throw new TestException(TestHelper.getStackTrace(e));
  }
      }
    }
  }
  
  protected void doStopStartVmAndQueryOperation() {
    SharedCounters counters = ParRegQueryBB.getBB().getSharedCounters();
    if (serialExecution && counters.read(ParRegQueryBB.stopStartVmsCounter) < totalNumOfVmsToStop ) {
      // stop vm after a few operations
      if (counters.read(ParRegQueryBB.operationCount)< 7) 
        return;

      StopStartVMs.stopStartOtherVMs(2);

      counters.increment(ParRegQueryBB.stopStartVmsCounter);
      counters.zero(ParRegQueryBB.operationCount);
 
      Log.getLogWriter().info("Successfully recycled vm");
      doEntryAndQueryOperations();
      //MasterController.sleepForMs(20000); 
      //Log.getLogWriter().info("Finished sleeping of 20sec");
    }
    else if (!serialExecution) {
      if (counters.read(ParRegQueryBB.queryCount)> 7 ) {
        if (counters.incrementAndRead(ParRegQueryBB.stopStartSignal) == 1) {
          
          List<ClientVmInfo> vms = stopStartVMs(1);
          PRObserver.waitForRebalRecov(vms, 1, numOfRegions, null, null, false);

          counters.zero(ParRegQueryBB.queryCount);
          counters.zero(ParRegQueryBB.stopStartSignal); 
          
          doQuery(false); //false, logAddition
        } 
      } //for concurrent execution
      
    }
  }
   
  private List<ClientVmInfo> stopStartVMs(final int numToTarget) {
    Object[] tmpArr = StopStartVMs.getOtherVMsWithExclude(numToTarget, "locator");
    // get the VMs to stop; vmList and stopModeList are parallel lists
    List<ClientVmInfo> vmList = (List<ClientVmInfo>)(tmpArr[0]);
    List stopModeList = (List)(tmpArr[1]);
    StopStartVMs.stopStartVMs(vmList, stopModeList);
    return vmList;
  }

  /* override methods */
  /* ======================================================================== */
  
  protected void createAllIndex() {
    String objectType = TestConfig.tab().stringAt(QueryPrms.objectType, "");
    if(!objectType.equals("parReg.query.NewPortfolio")) {
        return;
    }
    IndexTest indexTest = new IndexTest();
    for (int i = 0; i < numOfRegions; i++) {
      indexTest.createIndex(REGION_NAME + ("" + i));
    }
  }
  
  protected void createIndex() {
    String objectType = TestConfig.tab().stringAt(QueryPrms.objectType, "");
    if(!objectType.equals("parReg.query.NewPortfolio")) {
        return;
    }
    IndexTest indexTest = new IndexTest();
    int regionNumber = (new Random()).nextInt(numOfRegions); 
    indexTest.createIndex(REGION_NAME + ("" + regionNumber));
  }
  
  protected void removeIndex() {
    String objectType = TestConfig.tab().stringAt(QueryPrms.objectType, "");
    if(!objectType.equals("parReg.query.NewPortfolio")) {
        return;
    }
    IndexTest indexTest = new IndexTest();
    int regionNumber = (new Random()).nextInt(numOfRegions); 
    indexTest.removeIndex(REGION_NAME + ("" + regionNumber));
  }  
  
  protected int getNumVMsWithListeners() {
     return TestHelper.getNumVMs();
  }
  
  protected void addObject(Region aRegion, boolean logAddition){
      addObject(aRegion, logAddition, true);      
  }
  
  protected void addObject(Region aRegion, boolean logAddition, boolean verifyQuery) {
    String name = NameFactory.getNextPositiveObjectName();
    Object anObj = getObjectToAdd(name);
    String callback = createCallbackPrefix + ProcessMgr.getProcessId();
    if (logAddition)
       Log.getLogWriter().info("addObject: calling put for name " + name + ", object " + 
           TestHelper.toString(anObj) + " callback is " + callback + ", region is " + aRegion.getFullPath());
    try {
       aRegion.put(name, anObj, callback);
        Log.getLogWriter().info("Object " + ((NewPortfolio)anObj).getName() +
          " in " + aRegion.getFullPath() + " is " + ((NewPortfolio)anObj).getStatus());
       
       String key = aRegion.getName() + name;
       ParRegQueryBB.putToSharedMap(key, anObj);
       QueryBB.putQueryValidator(new QueryValidator("add", name, true, true, anObj));
    } catch (RegionDestroyedException e) {
       handleRegionDestroyedException(aRegion, e);
    }
    catch (Exception e) {
       throw new TestException(TestHelper.getStackTrace(e));
   } 
    EventBB.incrementCounter("EventBB.NUM_CREATE", EventBB.NUM_CREATE);
    if (verifyQuery) {
      validateQuery(aRegion);
    }
  }
  
  protected void invalidateObject(Region aRegion, boolean isLocalInvalidate) {
    Set aSet = aRegion.keySet();
    if (aSet.size() == 0) {
       Log.getLogWriter().info("invalidateObject: No names in region");
       return;
    }
    Iterator it = aSet.iterator();
    Object name = null;
    if (it.hasNext()) {
       name = it.next();
    } else { // has been destroyed cannot continue
       Log.getLogWriter().info("invalidateObject: Unable to get name from region");
       return; 
    }
    boolean containsValue = aRegion.containsValueForKey(name);
    boolean alreadyInvalidated = !containsValue;
    Log.getLogWriter().info("containsValue for " + name + ": " + containsValue);
    Log.getLogWriter().info("alreadyInvalidated for " + name + ": " + alreadyInvalidated);
    try {
       String callback = invalidateCallbackPrefix + ProcessMgr.getProcessId();
       if (isLocalInvalidate) {
          Log.getLogWriter().info("invalidateObject: local invalidate for " + name + " callback is " + callback);
          aRegion.localInvalidate(name, callback);
          Log.getLogWriter().info("invalidateObject: done with local invalidate for " + name);
          if (!alreadyInvalidated) {
            EventBB.incrementCounter("EventBB.NUM_LOCAL_INVALIDATE", EventBB.NUM_LOCAL_INVALIDATE);
            QueryBB.putQueryValidator(new QueryValidator("localInvalidate", name, true, false, null));
          }
       } else {
          Log.getLogWriter().info("invalidateObject: invalidating name " + name + " callback is " + callback);
          aRegion.invalidate(name, callback);
          
          String key = aRegion.getName() + name;
          ParRegQueryBB.getBB().getSharedMap().put(key, null);   //can write null onto hydra sharedMap
          Log.getLogWriter().info("invalidateObject: done invalidating name " + name);
          if (!alreadyInvalidated) {
             EventBB.incrementCounter("EventBB.NUM_INVALIDATE", EventBB.NUM_INVALIDATE);
             QueryBB.putQueryValidator(new QueryValidator("invalidate", name, true, false, null));
          }
       }
       if (isCarefulValidation)
          verifyObjectInvalidated(aRegion, name);
    } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
       if (isCarefulValidation)
          throw new TestException(TestHelper.getStackTrace(e));
       else {
          Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
          return;
       }
    } 
    
     validateQuery(aRegion);
  }
  
  protected void destroyObject(Region aRegion, boolean isLocalDestroy) {
    Set aSet = aRegion.keySet();
    Iterator iter = aSet.iterator();
    if (!iter.hasNext()) {
       Log.getLogWriter().info("destroyObject: No names in region");
       return;
    }
    try {
       Object name = iter.next();
       destroyObject(aRegion, name, isLocalDestroy);
    } catch (NoSuchElementException e) {
       throw new TestException("Bug 30171 detected: " + TestHelper.getStackTrace(e));
    }
     validateQuery(aRegion);
  }
  
  private void destroyObject(Region aRegion, Object name, boolean isLocalDestroy) {
    try {
       String callback = destroyCallbackPrefix + ProcessMgr.getProcessId();
       if (isLocalDestroy) {
          Log.getLogWriter().info("destroyObject: local destroy for " + name + " callback is " + callback);
          aRegion.localDestroy(name, callback);
          Log.getLogWriter().info("destroyObject: done with local destroy for " + name);
          EventBB.incrementCounter("EventBB.NUM_LOCAL_DESTROY", EventBB.NUM_LOCAL_DESTROY);
          QueryBB.putQueryValidator(new QueryValidator("localDestroy", name, false, false, null));
       } else {
          Log.getLogWriter().info("destroyObject: destroying name " + name + " callback is " + callback);
          aRegion.destroy(name, callback);
          
          String key = aRegion.getName() + name;
          ParRegQueryBB.getBB().getSharedMap().remove(key);
          
          Log.getLogWriter().info("destroyObject: done destroying name " + name);
          EventBB.incrementCounter("EventBB.NUM_DESTROY", EventBB.NUM_DESTROY);
          QueryBB.putQueryValidator(new QueryValidator("destroy", name, false, false, null));
       }
    } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
       if (isCarefulValidation)
          throw new TestException(TestHelper.getStackTrace(e));
       else {
          Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
          return;
       }
    } 
  }
  
  
  protected void updateObject(Region aRegion) {
    Set aSet = aRegion.keySet();
    Iterator iter = aSet.iterator();
    if (!iter.hasNext()) {
       Log.getLogWriter().info("updateObject: No names in region");
       return;
    }
    Object name = iter.next();
    updateObject(aRegion, name);
    
    validateQuery(aRegion);
  }
  
  /**
   * Updates the entry with the given key (<code>name</code>) in the
   * given region.
   */
  protected void updateObject(Region aRegion, Object name) {
   Object anObj = null;
   try {
      anObj = aRegion.get(name);
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   Object newObj = getUpdateObject((String)name);
   try {
      String callback = updateCallbackPrefix + ProcessMgr.getProcessId();
      Log.getLogWriter().info("updateObject: replacing name " + name + " with " + 
         TestHelper.toString(newObj) + "; old value is " + TestHelper.toString(anObj) +
         ", callback is " + callback);
      aRegion.put(name, newObj, callback);
      
      String key = aRegion.getName() + name;
      ParRegQueryBB.putToSharedMap(key, newObj);
      Log.getLogWriter().info("Done with call to put (update)");
   } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   QueryBB.putQueryValidator(new QueryValidator("update", name, true, true, newObj));
   EventBB.incrementCounter("EventBB.NUM_UPDATE", EventBB.NUM_UPDATE);
  }
  
  protected void readObject(Region aRegion) {
     super.updateObject(aRegion);
     validateQuery(aRegion);
  }
  
  protected Object getObjectToAdd(String name) {
    
    boolean useRandomValues = false;
    useRandomValues =  TestConfig.tab().booleanAt(QueryPrms.useRandomValues, false);
    if (useRandomValues) { 
      BaseValueHolder anObj = new ValueHolder(name, randomValues);
      return anObj;
    } else {
      long i = NameFactory.getPositiveNameCounter();
      int index = (int)(i % maxId);        //duplicate id allowed for testing query results
      String className = TestConfig.tab().stringAt(QueryPrms.objectType);
      NewPortfolio val = null;
      if (className.equals(NewPortfolio.class.getName())) {
        val = new NewPortfolio(name, index);
      } else if (className.equals("parReg.query.PdxVersionedNewPortfolio") ||
                 className.equals("parReg.query.VersionedNewPortfolio")) {
        val = PdxTest.getVersionedNewPortfolio(className, name, index);
      } else {
        throw new TestException("Unknown objectType " + className);
      }
      Log.getLogWriter().info("set portfolio done");
      return val;
    }
  }
  
  protected Object getUpdateObject(String name) {
    return getObjectToAdd(name);
  }
  
  protected void validateQuery(Region aRegion) {
    for (int i=0; i<numOfQueryStrings; i++) {
      validateQuery(aRegion, i);        //validate all the queries
    }
    
  }
  
  /* ======================================================================== */
  /**
   * To verify the results from query and from BB are same.
   */
  protected void validateQuery(Region aRegion, int i) {
    Log.getLogWriter().info("Performing the query validation check"); 
    String regionName = aRegion.getName();
    Log.getLogWriter().info("Region name is : " + regionName ); 
    Object result;
    //int i;
    String region1 = randRgn();   //not used for now, could be used for future when multiple regions are supported
    
    try {
        String queries[] = {
           "select distinct * from " + aRegion.getFullPath() + " where status = 'active'",  
           "select distinct * from " + aRegion.getFullPath() + " where NOT (id <= 5 AND status <> 'active')",
           "select distinct * from " + aRegion.getFullPath() + " p where p.id IN " + 
                "(select distinct np.id from " + region1 + " np where np.status = 'active')", //does not support yet
           "import parReg.\"query\".Position; select distinct r from " + aRegion.getFullPath() + " r, " +
                "r.positions.values pVal TYPE Position where r.status = 'active' AND pVal.mktValue >= 25.00",
           "select distinct status from " + aRegion.getFullPath() + " where id > 5 AND status = 'active'",
           "select distinct status, name from " +  aRegion.getFullPath() + " where \"type\" = 'type1'",
           "import parReg.\"query\".Position; select distinct r.name, pVal, r.status from " + aRegion.getFullPath() + " r , " +
               "r.positions.values pVal TYPE Position " +
               "where r.\"type\" = 'type1' AND pVal.mktValue >= 25.00 AND pVal.qty > 500",
           "import parReg.\"query\".Position; select distinct r.name, pVal, r.status from " + aRegion.getFullPath() + " r , " +
               "r.positions.values pVal TYPE Position ",
            "import parReg.\"query\".Position; select distinct r.name, pVal, r.\"type\" from " + aRegion.getFullPath() + " r , " +
               "r.positions.values pVal TYPE Position " + 
               "where pVal.mktValue >=1.00 AND (r.name='Object_11' OR r.name='Object_12')",              
        };
       //Add the limitClause if configured to do so
       if (queryLimit != -1) {
         for(int q =0; q < queries.length; q++) {
           queries[q] += " limit " + queryLimit;
         }       
       } 
       //i = ((new Random()).nextInt(queries.length));
       
       Cache aCache = CacheHelper.getCache();
       if (aCache == null) { // could be null in an HA test
         return;
       }
       Query query = aCache.getQueryService().newQuery(queries[i]);
       Log.getLogWriter().info("query with index " + i + " " + query.toString()); 
       // for pdx tests, we want the execute to occur without having domain classes referenced from the 
       //     class loader IF the test is using pdxReadSerialized
       // if this is a pdx test, this thread currently has the domain class in its class loader
       ClassLoader previousCL = Thread.currentThread().getContextClassLoader();
       if (pdxReadSerialized) { // remove the domain classes from this thread's class loader
         Log.getLogWriter().info("Setting class loader to remove domain classes in preparation of a query execute: " + origClassLoader.get());
         Thread.currentThread().setContextClassLoader((ClassLoader) origClassLoader.get());
       }
       try {
         Cache theCache = CacheHelper.getCache();
         if (theCache != null) { // could be null during HA
           // clear the pdx type registry so with Darrel's new 662 changes we do not have domain classes available during the query
           // note that for a serial test, this will clear the pdx registry and it will remain cleared durin the query execution
           // but for concurrent tests, other threads might put domain classes back into the registry while the query is executing
           Log.getLogWriter().info("Clearing the pdx registry with a test hook");
           ((GemFireCacheImpl)theCache).getPdxRegistry().flushCache();
         }
         Log.getLogWriter().info("Executing query " + queries[i]);
         result = query.execute();
         Log.getLogWriter().info("Done executing query " + queries[i]);
       } finally {
         if (pdxReadSerialized) { // set the class loader back to one that includes pdx domain classes
           Log.getLogWriter().info("Setting class loader back to one that refers to domain classes: " + previousCL);
           Thread.currentThread().setContextClassLoader(previousCL);
         }
       }

       validateQueryLimit(result); 
    } catch (QueryInvocationTargetException ite) {
       if (ite.toString().indexOf("Unable to retrieve") >= 0) {
         throw new TestException(TestHelper.getStackTrace(ite));
       }
       if (QueryPrms.allowQueryInvocationTargetException()) {
        // ignore for PR Query HA tests
        Log.getLogWriter().warning("Caught " + ite + " (expected with concurrent execution); continuing with test",ite.getRootCause());
        return;
       } else {
        throw new TestException("Caught exception during query execution" + TestHelper.getStackTrace(ite));
       }
    } catch (UnsupportedOperationException e){
      Log.getLogWriter().info("Multiple region reference is not supported, continuing test"); //if i==2
      return;
    } 
    catch (CancelException e) {
      throw e;
    }  catch (Exception e) {
      throw new TestException("Caught exception during query validation. " 
            + TestHelper.getStackTrace(e));
    }       
        Set dataFromBB = ParRegQueryBB.getBB().getSharedMap().getMap().entrySet();
             
        Map data = new HashMap();
        NewPortfolio aPF;
  
        String key;
        Iterator itr = dataFromBB.iterator();     //all portfolios are writen to bb's sharedMap
        
        if (i == 0) {        
          while (itr.hasNext()) {                //find all portfolios that meet requirements
            Map.Entry m = (Map.Entry) itr.next();
            key = (String) m.getKey();
            Object val = ParRegQueryBB.getFromSharedMap(key);
            aPF = (NewPortfolio) val;      
            if (key.startsWith(regionName + "Object") && aPF.getStatus().equals("active")) {
              
              key = aPF.getName();
              data.put(key, aPF);
            }
          }        
        } 
        else if (i == 1) {
          while (itr.hasNext()) {                //find all portfolio that meet requirements
            Map.Entry m = (Map.Entry) itr.next();
            key = (String) m.getKey();
            Object val = ParRegQueryBB.getFromSharedMap(key);
            aPF = (NewPortfolio) val;      
            if (key.startsWith(regionName + "Object") && !(aPF.getId() <=5 && !aPF.getStatus().equals("active"))) {
              
              key = aPF.getName();
              data.put(key, aPF);
            }
          }       
        }     
        else if (i == 2){
          return;
        /* the test will not be supported in this release as it has multiple regions
          veriification was developed for future use only
        
          int regionNum = 0;            
          for (i=0; i<numOfRegions; i++) {
            if (region1.equals("/" + REGION_NAME + i)) {  //need to change the scope in QueryTest to public
              regionNum = i;
            }
          }
          //get the region thru region number
          String secondRegionName = REGION_NAME + regionNum;
          
          Set aSet = new HashSet();
          while (itr.hasNext()) {                //find all portfolio that meet requirements
            Map.Entry m = (Map.Entry) itr.next();
            key = (String) m.getKey();
            aPF = (NewPortfolio) m.getValue();     
            if (key.startsWith(secondRegionName + "Object") && aPF.getStatus().equals("active")) {
              aSet.add(new Integer(aPF.getId())); //store the qualified id from the second query
            }        
          }
          
          itr = dataFromBB.iterator();    //start a new iteration
          while (itr.hasNext()) {                //find all portfolio that meet requirements
            Map.Entry m = (Map.Entry) itr.next();
            key = (String) m.getKey();
            aPF = (NewPortfolio) m.getValue();     
            if (key.startsWith(regionName + "Object") && aSet.contains(new Integer(aPF.getId()))) {
              key = aPF.getName();
              data.put(key, aPF);
            }        
          }
          Log.getLogWriter().info("Start to validate the third query");
          */
  
        }  
        else if (i == 3) {
          Position aPos;
          Map aMap;
          Set aSet;
          Iterator itr1;
          
          while (itr.hasNext()) {                
            Map.Entry m = (Map.Entry) itr.next();
            key = (String) m.getKey();
            Object val = ParRegQueryBB.getFromSharedMap(key);
            aPF = (NewPortfolio) val;      
  //            Log.getLogWriter().info("form bb, region name, key is " + key);
            if (key.startsWith(regionName + "Object") && aPF.getStatus().equals("active")) {
  //            Log.getLogWriter().info("query number 4, name is " +aPF.getName() + " status is " + aPF.getStatus());
                          
              aMap = aPF.getPositions();
              aSet = aMap.entrySet();
              itr1 = aSet.iterator();
              while (itr1.hasNext()) {                    //loop thru all the positions in a NewPortfolio
                Map.Entry posEntry = (Map.Entry) itr1.next();
                aPos = (Position) posEntry.getValue();
  //              Log.getLogWriter().info("Position: secId is " + aPos.getSecId() + " mktValue is " + aPos.getMktValue());
                if (aPos.getMktValue() >= 25.00) {
                  key = aPF.getName();
                  data.put(key, aPF);
                  break;
                }
              }          
            }
          }    
        }
        else if (i == 4) {               
          if (((SelectResults)result).size() > 1 ) {
            Log.getLogWriter().severe("result isa " + result.getClass());
            itr = ((SelectResults)result).iterator();
            int counter = 0;
            while (itr.hasNext()) {
              counter++;
              String status = (String)itr.next();
              Log.getLogWriter().info("query number 5, from result, status is " + status);   
            }
            Log.getLogWriter().info("query number 5, total from query results is " + counter);
            throw new TestException ("query number 5: distinct value should eliminate duplicates");                    
          }
          return;    
        } // to test distinct -- eliminate dumplicates
        else if (i == 5) {
          ObjectType oType = new ObjectTypeImpl (String.class);
          ObjectType[] oTypes = {oType, oType};
          String[] fieldNames = {"status", "name"};
          StructTypeImpl sType = new StructTypeImpl (fieldNames, oTypes);
          
          while (itr.hasNext()) {                //find all portfolios that meet requirements
            Map.Entry m = (Map.Entry) itr.next();
            key = (String) m.getKey();
            Object val = ParRegQueryBB.getFromSharedMap(key);
            aPF = (NewPortfolio) val;      
            if (key.startsWith(regionName + "Object") && aPF.getType().equals("type1")) {
              
              key = aPF.getName();
              Object[] objects = {aPF.getStatus(), aPF.getName()};
              Struct aStruct = new StructImpl(sType, objects);
              
              data.put(key, aStruct);     
              //aStruct needs to be unique to verify, otherwise needs to use hashset to test the distinct value
            }
          }        
         
          verifyStruct(data, result, i);
          return;
        }
        else if (i==6){
          Position aPos;
          Map aMap;
          Set aSet;
          Iterator itr1;
          
          ObjectType oType = new ObjectTypeImpl (String.class); 
          ObjectType oType1 = new ObjectTypeImpl (Position.class);
          ObjectType[] oTypes = {oType, oType1, oType};
          String[] fieldNames = {"name", "pVal", "status"};       
          StructTypeImpl sType = new StructTypeImpl (fieldNames, oTypes);
          
          while (itr.hasNext()) {                
            Map.Entry m = (Map.Entry) itr.next();
            key = (String) m.getKey();
            Object val = ParRegQueryBB.getFromSharedMap(key);
            aPF = (NewPortfolio) val;      
            if (key.startsWith(regionName + "Object") && aPF.getType().equals("type1")) {
                          
              aMap = aPF.getPositions();
              aSet = aMap.entrySet();
              itr1 = aSet.iterator();
              while (itr1.hasNext()) {                    //loop thru all the positions in a NewPortfolio
                Map.Entry posEntry = (Map.Entry) itr1.next();
                aPos = (Position) posEntry.getValue();
  //              Log.getLogWriter().info("Position: secId is " + aPos.getSecId() + " mktValue is " + aPos.getMktValue());
                if (aPos.getMktValue() >= 25.00 && aPos.getQty() > 500) {
                  key = NameFactory.getNextPositiveObjectName();
                  
                  Object[] objects = {aPF.getName(), aPos, aPF.getStatus()};
                  Struct aStruct = new StructImpl(sType, objects);
  /*                Log.getLogWriter().info("In BB, following struct meets the criteria: "
                      + "name = " + aPF.getName() + " status = " + aPF.getStatus() + " type = " + aPF.getType()
                      + " mktValue = " + aPos.getMktValue() + " qty = " + aPos.getQty());
  */                
                  data.put(key, aStruct);
                } 
              }          
            }
          } 
          verifyStruct(data, result, i);
          return;
        }
        else if (i==7){
          Position aPos;
          Map aMap;
          Set aSet;
          Iterator itr1;
          
          ObjectType oType = new ObjectTypeImpl (String.class); 
          ObjectType oType1 = new ObjectTypeImpl (Position.class);
          ObjectType[] oTypes = {oType, oType1, oType};
          String[] fieldNames = {"name", "pVal", "status"};       
          StructTypeImpl sType = new StructTypeImpl (fieldNames, oTypes);
          
          while (itr.hasNext()) {                
            Map.Entry m = (Map.Entry) itr.next();
            key = (String) m.getKey();
            Object val = ParRegQueryBB.getFromSharedMap(key);
            aPF = (NewPortfolio) val;      
            if (key.startsWith(regionName + "Object")) {
                          
              aMap = aPF.getPositions();
              aSet = aMap.entrySet();
              itr1 = aSet.iterator();
              while (itr1.hasNext()) {                    //loop thru all the positions in a NewPortfolio
                Map.Entry posEntry = (Map.Entry) itr1.next();
                aPos = (Position) posEntry.getValue();
  //              Log.getLogWriter().info("Position: secId is " + aPos.getSecId() + " mktValue is " + aPos.getMktValue());
  
                  key = NameFactory.getNextPositiveObjectName();
                  
                  Object[] objects = {aPF.getName(), aPos, aPF.getStatus()};
                  Struct aStruct = new StructImpl(sType, objects);
  /*                Log.getLogWriter().info("In BB, following struct meets the criteria: "
                      + "name = " + aPF.getName() + " status = " + aPF.getStatus() + " type = " + aPF.getType()
                      + " mktValue = " + aPos.getMktValue() + " qty = " + aPos.getQty());
  */                
                  data.put(key, aStruct);
                }
              }          
            }
          verifyStruct(data, result, i);
          return;
        }
        else if (i==8){
          Position aPos;
          Map aMap;
          Set aSet;
          Iterator itr1;
          
          ObjectType oType = new ObjectTypeImpl (String.class); 
          ObjectType oType1 = new ObjectTypeImpl (Position.class);
          ObjectType[] oTypes = {oType, oType1, oType};
          String[] fieldNames = {"name", "pVal", "type"};       
          StructTypeImpl sType = new StructTypeImpl (fieldNames, oTypes);
          
          while (itr.hasNext()) {                
            Map.Entry m = (Map.Entry) itr.next();
            key = (String) m.getKey();
            Object val = ParRegQueryBB.getFromSharedMap(key);
            aPF = (NewPortfolio) val;      
            if (key.startsWith(regionName + "Object") && 
                (aPF.getName().equals("Object_12") || aPF.getName().equals("Object_11"))) {
                          
              aMap = aPF.getPositions();
              aSet = aMap.entrySet();
              itr1 = aSet.iterator();
              while (itr1.hasNext()) {                    //loop thru all the positions in a NewPortfolio
                Map.Entry posEntry = (Map.Entry) itr1.next();
                aPos = (Position) posEntry.getValue();
  //              Log.getLogWriter().info("Position: secId is " + aPos.getSecId() + " mktValue is " + aPos.getMktValue());
                if (aPos.getMktValue() >= 1.00) {
                  key = NameFactory.getNextPositiveObjectName();
                  
                  Object[] objects = {aPF.getName(), aPos, aPF.getType()};
                  Struct aStruct = new StructImpl(sType, objects);
  /*                Log.getLogWriter().info("In BB, following struct meets the criteria: "
                      + "name = " + aPF.getName() + " status = " + aPF.getStatus() + " type = " + aPF.getType()
                      + " mktValue = " + aPos.getMktValue() + " qty = " + aPos.getQty());
  */                
                  data.put(key, aStruct);
                } 
              }          
            }
          } 
          verifyStruct(data, result, i);
          return;
        }
          
        Log.getLogWriter().info("qualified size from BB is " + data.size()); 
        //verifyResult(data, result, i);
        verifyResult(data, result, i , aRegion);       
  }
  
  //for testing purpose
  protected void verifyResult (Map dataFromBB, Object result, int i, Region aRegion) {
    i++; //the ith query
    Log.getLogWriter().info("Verifying query number " + i);
    int dataSizeFromBB = dataFromBB.size();
    int dataSizeFromQuery = ((SelectResults)result).size();
   
    if (dataSizeFromBB > dataSizeFromQuery) {
      Log.getLogWriter().info("Operation Failed");
      if (i == 1) logResults(dataFromBB, result, aRegion);
      //Log.getLogWriter().info(resultsToString(dataFromBB, result, aRegion));
      
        
      throw new TestException("Size in the BB " + dataSizeFromBB +
          " is more than that in the Query result size: " + dataSizeFromQuery +
          " while executing query number " + i);  
    } 
    else if (dataSizeFromBB < dataSizeFromQuery) {
      Log.getLogWriter().info("Operation Failed");
      // if (i == 1) logResults(dataFromBB, result, aRegion);
      
      throw new TestException("Size in the BB " + dataSizeFromBB +
          " is fewer than that in the Query result size: " + dataSizeFromQuery +
          " while executing query number " + i);  
    } 
    
    Iterator itr = ((SelectResults)result).iterator();
  
    while (itr.hasNext()) { 
      Object currObj = itr.next();
      if (currObj instanceof PdxInstance) {
        currObj = PdxTestVersionHelper.toBaseObject(currObj);
      }
      NewPortfolio aPF = (NewPortfolio)currObj;
      
      if (!dataFromBB.containsValue(aPF)){
        Log.getLogWriter().info("Query result contains item not found in the BB" +
            " while executing query number " + i);  
        if (i == 1) logResults(dataFromBB, result, aRegion);
        throw new TestException("Query result contains item not found in the BB" + 
            " while executing query number " + i);   
      }    
    }   
    
    Log.getLogWriter().info("Operation done : query " + i + " verified successfully");       
  }
  
  private String resultsToString(Map dataFromBB, Object result, Region aRegion) {
    StringBuffer aStr = new StringBuffer();
    aStr.append("Data from BB has " + dataFromBB.size() + " entries\n" +
                "results has " + ((SelectResults)result).size() + " entries\n");
    aStr.append("from bb\n");
    for (Object key: dataFromBB.keySet()) {
      aStr.append("   " + key + " " + dataFromBB.get(key) + "\n");
    }
    
    aStr.append("results\n");
    Iterator itr = ((SelectResults)result).iterator();
    while (itr.hasNext()) {
      aStr.append("   " + itr.next() + "\n");
    }
    return aStr.toString();
  }

  //for testing purpose
  protected void logResults(Map dataFromBB, Object result, Region aRegion) {
    Iterator itr = ((SelectResults)result).iterator();
    while (itr.hasNext()) {
      NewPortfolio aPF = (NewPortfolio)(PdxTestVersionHelper.toBaseObject(itr.next()));
      if (aPF.getStatus().compareTo("active") == 0) {
       // Log.getLogWriter().info("From query result this object " + aPF.getName() + " status is active" );
      }      
    }
    
    Collection data = dataFromBB.values();
    
    int counter =0;
    itr = aRegion.values().iterator();
    while (itr.hasNext()) {     
      NewPortfolio aPF = (NewPortfolio)(PdxTestVersionHelper.toBaseObject(itr.next()));
      if (aPF.getStatus().compareTo("active") == 0) {        
        counter++;
        if (data.contains(aPF)){
          data.remove(aPF);
        }
     //   Log.getLogWriter().info("From the region this object " + aPF.getName() + " status is active" );
      }      
    }
    Log.getLogWriter().info("From the region the size of qualifying results is " + counter);  
    
    itr = data.iterator();
    while (itr.hasNext()){
      Log.getLogWriter().info("Object in blackboard "+
         ((NewPortfolio)PdxTestVersionHelper.toBaseObject(itr.next())).getName());
    }
  }
  
  /**
   * Verify the data meets the condintions from BB are the same as from queryResult.
   */
  protected boolean verifyResult (Map dataFromBB, Object result, int i) {
    i++; //the ith query
    Log.getLogWriter().info("Verifying query number " + i);
   
    if (dataFromBB.size() > ((SelectResults)result).size()) {
      Log.getLogWriter().info("Operation Failed");
      throw new TestException("Size in the BB " + dataFromBB.size() +
          " is more than that in the Query result size: " + ((SelectResults)result).size() +
          " while executing query number " + i);  
    } 
    else if (dataFromBB.size() < ((SelectResults)result).size()) {
      Log.getLogWriter().info("Operation Failed");
      throw new TestException("Size in the BB " + dataFromBB.size() +
          " is fewer than that in the Query result size: " + ((SelectResults)result).size() +
          " while executing query number " + i);  
    } 
    
    Iterator itr = ((SelectResults)result).iterator();
  
    while (itr.hasNext()) { 
      NewPortfolio aPF = (NewPortfolio)itr.next();
      
      if (!dataFromBB.containsValue(aPF)){
        Log.getLogWriter().info("Query result contains item not found in the BB" +
            " while executing query number " + i);  
        throw new TestException("Query result contains item not found in the BB" + 
            " while executing query number " + i);   
      }    
    }   
    
    Log.getLogWriter().info("Operation done : query " + i + " verified successfully");    
    return true;
  }
  
  //verify query have projections -- Struct
  protected boolean verifyStruct(Map dataFromBB, Object result, int i) {
    i++; //the ith query
    Log.getLogWriter().info("Verifying query number " + i);
    Log.getLogWriter().info("qualified size from BB is " + dataFromBB.size()); 
    
    if (dataFromBB.size() > ((SelectResults)result).size()) {
      Log.getLogWriter().info("Size in the BB " + dataFromBB.size() +
          " is more than that in the Query result size: " + ((SelectResults)result).size() +
          " while executing query number " + i);
      logStructResults(dataFromBB, result);
      throw new TestException("Size in the BB " + dataFromBB.size() +
          " is more than that in the Query result size: " + ((SelectResults)result).size() +
          " while executing query number " + i);  
  
    } 
    else if (dataFromBB.size() < ((SelectResults)result).size()) {
      Log.getLogWriter().info("Size in the BB " + dataFromBB.size() +
          " is fewer than that in the Query result size: " + ((SelectResults)result).size() +
          " while executing query number " + i);
      logStructResults(dataFromBB, result);
      throw new TestException("Size in the BB " + dataFromBB.size() +
          " is fewer than that in the Query result size: " + ((SelectResults)result).size() +
          " while executing query number " + i);  
    } 
    
    Iterator itr = ((SelectResults)result).iterator();
 //   Iterator itr1 = dataFromBB.values().iterator();
 //   Log.getLogWriter().info("dataFromBB class is " + dataFromBB.getClass());
    
 /*   while (itr1.hasNext()){
      StructImpl bbStruct = (StructImpl) itr1.next();
      Log.getLogWriter().info("From BB, bbStruct: " +bbStruct.toString());
      Log.getLogWriter().info("From BB, field name 1 " + bbStruct.getFieldNames()[0].toString());
      Log.getLogWriter().info("From BB, field name 2 " + bbStruct.getFieldNames()[1].toString());
      Log.getLogWriter().info("From BB, Object type 1 " + bbStruct.getFieldTypes()[0].toString());
      Log.getLogWriter().info("From BB, Object type 1 " + bbStruct.getFieldTypes()[1].toString());
      Log.getLogWriter().info("From BB, Object value 1 " + bbStruct.getFieldValues()[0].toString());
      Log.getLogWriter().info("From BB, Object value 2 " + bbStruct.getFieldValues()[1].toString()); 
    }
 */   
    while (itr.hasNext()) { 
      StructImpl aStruct = (StructImpl)itr.next();
  /*    
      Log.getLogWriter().info("From result set, a Struct: " +aStruct.toString());
      Log.getLogWriter().info("From result set, field name 1 " + aStruct.getFieldNames()[0].toString());
      Log.getLogWriter().info("From result set, field name 2 " + aStruct.getFieldNames()[1].toString());
      Log.getLogWriter().info("From result set, field type 1 " + aStruct.getFieldTypes()[0].toString());
      Log.getLogWriter().info("From result set, field type 2 " + aStruct.getFieldTypes()[1].toString());
      Log.getLogWriter().info("From result set, field value 1 " + aStruct.getFieldValues()[0].toString());
      Log.getLogWriter().info("From result set, field value 2 " + aStruct.getFieldValues()[1].toString());
  */
      
      if (!containsValue(dataFromBB, aStruct)){
        Log.getLogWriter().info("Query result contains item not found in the BB" +
            " while executing query number " + i + ", struct not found is " + aStruct);  
        logStructResults(dataFromBB, result);
        throw new TestException("Query result contains item not found in the BB" + 
            " while executing query number " + i);   
      }    
    }   
    
    Log.getLogWriter().info("Operation done : query " + i + " verified successfully");    
    return true;
  
  }
  
  /** Determine if aStruct is contained in dataFromBB on an equals basis.
   *  The possibility of PdxInstances inside the struct means that an equals
   *  comparison must be done by looking at the values in the struct. Since
   *  we do not support comparisons of PdxInstances to their domain object
   *  equivalents, we must handle testing for equality of a struct by looking
   *  inside the Struct.
   */
  protected boolean containsValue(Map dataFromBB, Struct aStruct) {
     if (pdxReadSerialized) {
       for (Object bbValue: dataFromBB.values()) {
          if (bbValue instanceof Struct) {
             Struct bbStruct = (Struct)bbValue;
             String[] bbFieldNames = bbStruct.getStructType().getFieldNames();
             String[] structFieldNames = aStruct.getStructType().getFieldNames();
             if (Arrays.equals(bbFieldNames, structFieldNames)) {
                Object[] bbFieldValues = bbStruct.getFieldValues();
                for (int i = 0; i < bbFieldValues.length; i++) {
                    bbFieldValues[i] = PdxTestVersionHelper.toBaseObject(bbFieldValues[i]);
                }
                Object[] structFieldValues = aStruct.getFieldValues();
                for (int i = 0; i < structFieldValues.length; i++) {
                    structFieldValues[i] = PdxTestVersionHelper.toBaseObject(structFieldValues[i]);
                }
                if (Arrays.equals(bbFieldValues, structFieldValues)) {
                   return true;
                }
             }
          }
       }
       return false;
     } else {
       return dataFromBB.containsValue(aStruct);
     }
  }

  //log info for testing
  protected void logStructResults (Map dataFromBB, Object result) {
    Iterator itr = ((SelectResults)result).iterator();
    Iterator itr1 = dataFromBB.values().iterator();
    Log.getLogWriter().info("dataFromBB class is " + dataFromBB.getClass());
       
    while (itr1.hasNext()){
       StructImpl bbStruct = (StructImpl) itr1.next();
       Log.getLogWriter().info("From BB, bbStruct: " +bbStruct.toString());
       Log.getLogWriter().info("From BB, field name 1 " + bbStruct.getFieldNames()[0].toString());
       Log.getLogWriter().info("From BB, field name 2 " + bbStruct.getFieldNames()[1].toString());
       Log.getLogWriter().info("From BB, Object type 1 " + bbStruct.getFieldTypes()[0].toString());
       Log.getLogWriter().info("From BB, Object type 1 " + bbStruct.getFieldTypes()[1].toString());
       Log.getLogWriter().info("From BB, Object value 1 " + bbStruct.getFieldValues()[0].toString());
       Log.getLogWriter().info("From BB, Object value 2 " + bbStruct.getFieldValues()[1].toString()); 
     }
    
     while (itr.hasNext()) { 
       StructImpl aStruct = (StructImpl)itr.next();
     
       Log.getLogWriter().info("From result set, a Struct: " +aStruct.toString());
       Log.getLogWriter().info("From result set, field name 1 " + aStruct.getFieldNames()[0].toString());
       Log.getLogWriter().info("From result set, field name 2 " + aStruct.getFieldNames()[1].toString());
       Log.getLogWriter().info("From result set, field type 1 " + aStruct.getFieldTypes()[0].toString());
       Log.getLogWriter().info("From result set, field type 2 " + aStruct.getFieldTypes()[1].toString());
       Log.getLogWriter().info("From result set, field value 1 " + aStruct.getFieldValues()[0].toString());
       Log.getLogWriter().info("From result set, field value 2 " + aStruct.getFieldValues()[1].toString());
     }
  }
  
  /** Check all counters from EventCountersBB. This verifies that 
   *  all events were distributed to all VMs (full distribution) and
   *  that no region or local events occurred.
   */
  protected void checkEventCounters() {
     checkEventCounters(true);
  }
  
  /** Check event counters. If numCloseIsExact is true, then the number of
   *  close events must be an exact match, otherwise allow numClose events
   *  to be the minimum of the expected numClose counter. This is useful in
   *  tests where the timing of shutting down the VMs/C clients may or may
   *  not cause a close event.
   *
   *  @param numCloseIsExact True if the numClose event counters must exactly
   *         match the expected numClose value, false if the numClose event
   *         counters must be no less than the expected numClose counter.
   */
  protected void checkEventCounters(boolean numCloseIsExact) {
     SharedCounters counters = EventBB.getBB().getSharedCounters();
     long numCreate = counters.read(EventBB.NUM_CREATE);
     long numUpdate = counters.read(EventBB.NUM_UPDATE);
     long numDestroy = counters.read(EventBB.NUM_DESTROY);
     long numInval = counters.read(EventBB.NUM_INVALIDATE);
     long numRegionDestroy = counters.read(EventBB.NUM_REGION_DESTROY);
     long numRegionInval = counters.read(EventBB.NUM_REGION_INVALIDATE);
     long numLocalDestroy = counters.read(EventBB.NUM_LOCAL_DESTROY);
     long numLocalInval = counters.read(EventBB.NUM_LOCAL_INVALIDATE);
     long numLocalRegionDestroy = counters.read(EventBB.NUM_LOCAL_REGION_DESTROY);
     long numLocalRegionInval = counters.read(EventBB.NUM_LOCAL_REGION_INVALIDATE);
     long numClose = counters.read(EventBB.NUM_CLOSE);
  
     int numVmsWithList = getNumVMsWithListeners();
     
     Log.getLogWriter().info("num VMs/C clients with listener installed: " + numVmsWithList);
  
     ArrayList al = new ArrayList();
          // afterCreate counters
             al.add(new ExpCounterValue("numAfterCreateEvents_isDist", (numCreate * numVmsWithList)));
             al.add(new ExpCounterValue("numAfterCreateEvents_isNotDist", 0)); 
             al.add(new ExpCounterValue("numAfterCreateEvents_isExp", 0)); 
             al.add(new ExpCounterValue("numAfterCreateEvents_isNotExp", (numCreate * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterCreateEvents_isRemote", (numCreate * (numVmsWithList - 1))));
             al.add(new ExpCounterValue("numAfterCreateEvents_isNotRemote", numCreate));
             al.add(new ExpCounterValue("numAfterCreateEvents_isLoad", 0)); 
             al.add(new ExpCounterValue("numAfterCreateEvents_isNotLoad", (numCreate * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterCreateEvents_isLocalLoad", 0)); 
             al.add(new ExpCounterValue("numAfterCreateEvents_isNotLocalLoad", (numCreate * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterCreateEvents_isNetLoad", 0)); 
             al.add(new ExpCounterValue("numAfterCreateEvents_isNotNetLoad", (numCreate * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterCreateEvents_isNetSearch", 0)); 
             al.add(new ExpCounterValue("numAfterCreateEvents_isNotNetSearch", (numCreate * numVmsWithList))); 
  
          // afterDestroy counters
             al.add(new ExpCounterValue("numAfterDestroyEvents_isDist", (numDestroy * numVmsWithList)));
             al.add(new ExpCounterValue("numAfterDestroyEvents_isNotDist", 0)); 
             al.add(new ExpCounterValue("numAfterDestroyEvents_isExp", 0)); 
             al.add(new ExpCounterValue("numAfterDestroyEvents_isNotExp", (numDestroy * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterDestroyEvents_isRemote", (numDestroy * (numVmsWithList - 1))));
             al.add(new ExpCounterValue("numAfterDestroyEvents_isNotRemote", numDestroy));
             al.add(new ExpCounterValue("numAfterDestroyEvents_isLoad", 0)); 
             al.add(new ExpCounterValue("numAfterDestroyEvents_isNotLoad", (numDestroy * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterDestroyEvents_isLocalLoad", 0)); 
             al.add(new ExpCounterValue("numAfterDestroyEvents_isNotLocalLoad", (numDestroy * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterDestroyEvents_isNetLoad", 0)); 
             al.add(new ExpCounterValue("numAfterDestroyEvents_isNotNetLoad", (numDestroy * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterDestroyEvents_isNetSearch", 0)); 
             al.add(new ExpCounterValue("numAfterDestroyEvents_isNotNetSearch", (numDestroy * numVmsWithList))); 
  
          // afterInvalidate counters
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isDist", (numInval * numVmsWithList)));
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotDist", 0)); 
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isExp", 0)); 
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotExp", (numInval * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isRemote", (numInval * (numVmsWithList - 1))));
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotRemote", numInval));
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isLoad", 0)); 
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotLoad", (numInval * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isLocalLoad", 0)); 
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotLocalLoad", (numInval * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isNetLoad", 0)); 
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotNetLoad", (numInval * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isNetSearch", 0)); 
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotNetSearch", (numInval * numVmsWithList))); 
  
          // afterUpdate counters
             al.add(new ExpCounterValue("numAfterUpdateEvents_isDist", (numUpdate * numVmsWithList)));
             al.add(new ExpCounterValue("numAfterUpdateEvents_isNotDist", 0)); 
             al.add(new ExpCounterValue("numAfterUpdateEvents_isExp", 0)); 
             al.add(new ExpCounterValue("numAfterUpdateEvents_isNotExp", (numUpdate * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterUpdateEvents_isRemote", (numUpdate * (numVmsWithList - 1))));
             al.add(new ExpCounterValue("numAfterUpdateEvents_isNotRemote", numUpdate));
             al.add(new ExpCounterValue("numAfterUpdateEvents_isLoad", 0)); 
             al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLoad", (numUpdate * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterUpdateEvents_isLocalLoad", 0)); 
             al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLocalLoad", (numUpdate * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterUpdateEvents_isNetLoad", 0)); 
             al.add(new ExpCounterValue("numAfterUpdateEvents_isNotNetLoad", (numUpdate * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterUpdateEvents_isNetSearch", 0)); 
             al.add(new ExpCounterValue("numAfterUpdateEvents_isNotNetSearch", (numUpdate * numVmsWithList))); 
  
          // afterRegionDestroy counters
             al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isDist", 0));
             al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotDist", 0)); 
             al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isExp", 0)); 
             al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotExp", 0));
             al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isRemote", 0));
             al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotRemote", 0));
  
          // afterRegionInvalidate counters
             al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isDist", (numRegionInval * numVmsWithList)));
             al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotDist", numLocalRegionInval)); 
             al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isExp", 0)); 
             al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotExp", ((numRegionInval * numVmsWithList) + numLocalRegionInval)));
             al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isRemote", (numRegionInval * (numVmsWithList - 1))));
             al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotRemote", (numRegionInval + numLocalRegionInval)));
  
          // afterRegionInvalidate counters
             al.add(new ExpCounterValue("numClose", numClose, numCloseIsExact));
  
     EventCountersBB.getBB().checkEventCounters(al);
  }

}
