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
package parReg.tx;

import parReg.ParRegBB;
import parReg.execute.PartitionObjectHolder;

import hydra.*;
import hydra.blackboard.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.*;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import java.util.*;
import util.*;

/**
 * A simple test class that supports executing entry operations on a PartitionRegion
 * while using a PartitionResolver:
 * - callbackResolver (encodes a PidRoutingObject as CallbackArgument)
 * - keyResolver (uses keys which are ModRoutingObjects)
 * - PartitionResolver (ModPartitionResolver uses key to partition mod numVms)
 *
 * When using a callbackResolver, this test class also creates a routingRegion to map
 * keys -> callbackArgs.   When a new entry is added, the (key, callbackArg) mapping
 * is saved in the routingRegion.  This same callbackArg is retrieved and used
 * as the callbackArg for any subsequent operations on key.
 *
 * In addition, with the CallbackResolver, a CallbackListener verifies that all 
 * events handled by the listener have CallbackArgs which route to the same
 * RoutingObject (based on this VMs pid).
 *
 * Tasks available (executed via FunctionExecution):
 * - ExecuteTx() - performs doEntryOperations as a tx (using FunctionService.onRegion())
 *
 * CloseTasks available (which are executed via FunctionExecution) include:
 * - GetAllMembersInDS (using FunctionService.onMembers())
 * - VerifyCustomPartitioningFunction (using FunctionService.onRegion())
 *
 * @see CallbackListener
 * @see ModRoutingObject
 * @see PidRoutingObject
 *
 * @see ExecuteTx
 * @see VerifyCustomPartitioningFunction
 * @see GetAllMembersInDS
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.1
 */
public class CustomPartitionTest extends util.OperationsClient {

  // Single instance in this VM
  static protected CustomPartitionTest testInstance;
  static Region testRegion;
  static Region routingRegion;

  private ArrayList errorMsgs = new ArrayList();
  private ArrayList errorException = new ArrayList();

 // CallbackResolver, KeyResolver or PartitionResolver
  private int customPartitionMethod;

  /**
   *  Create the cache and Region.  Check for forced disconnects (for gii tests)
   */
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new CustomPartitionTest();
      testInstance.initializeOperationsClient();

      try {
        testInstance.initializePrms();
        testInstance.initialize();
        testInstance.registerFunctions();
      } catch (Exception e) {
        Log.getLogWriter().info("initialize caught Exception " + e + ":" + e.getMessage());
        throw new TestException("initialize caught Exception " + TestHelper.getStackTrace(e));
      }  
    }
  }

  /* 
   * Creates cache and region (CacheHelper/RegionHelper)
   */
  protected void initialize() {

    // create cache/region (and connect to the DS)
    if (CacheHelper.getCache() == null) {
      CacheHelper.createCache(ConfigPrms.getCacheConfig());
    }

    // TestRegion
    String regionConfig = ConfigPrms.getRegionConfig();
    RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
    AttributesFactory factory = RegionHelper.getAttributesFactory(regionConfig);
    String regionName = rd.getRegionName();
    RegionHelper.createRegion(regionName, factory);
    testRegion = RegionHelper.getRegion(regionName);
    if (testRegion.getAttributes().getPoolName() != null) {
      testRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
      Log.getLogWriter().info("registered interest in ALL_KEYS for " + testRegion.getFullPath());
    }
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    if (bridgeConfig != null) {
      BridgeHelper.startBridgeServer(bridgeConfig);
    }

    // CallbackRegion (to save key->routingObject mapping)
    if (customPartitionMethod == PrTxPrms.CALLBACK_RESOLVER) {
      rd = RegionHelper.getRegionDescription("routingRegion");
      factory = RegionHelper.getAttributesFactory("routingRegion");
      regionName = rd.getRegionName();
      RegionHelper.createRegion(regionName, factory);
      routingRegion = RegionHelper.getRegion(regionName);
    }
  }

  protected void registerFunctions() {
    Function f = new VerifyCustomPartitioningFunction();
    FunctionService.registerFunction(f);

    f = new GetAllMembersInDS();
    FunctionService.registerFunction(f);

    f = new ExecuteTx();
    FunctionService.registerFunction(f);

    Log.getLogWriter().info("Registered functions: " + FunctionService.getRegisteredFunctions());
  }

  protected void initializePrms() {
    customPartitionMethod = PrTxPrms.getCustomPartitionMethod(); 
    Log.getLogWriter().info("Using customPartitionMethod " + PrTxPrms.customPartitionMethodToString(customPartitionMethod));
  }

  /**
   *  Populate regions with entries.  The test relies on being able to find the routing information via hashCode,
   *  so we'll need some data in all VMs prior to the executeTx task (which does an onRegion(pr).withFilter(key)).
   */
  public static void HydraTask_populateRegions() {
    Cache myCache = CacheHelper.getCache();
    Set allRegions = myCache.rootRegions();
    for (Iterator it = allRegions.iterator(); it.hasNext();) {
      Region aRegion = (Region)it.next();
      testInstance.populateRegion(aRegion);
    }
  }

  /*  Add at least numVm entries to the region, so we've satisfied all hashCodes.
   */
  protected void populateRegion(Region aRegion) {
    int numVms = TestConfig.getInstance().getTotalVMs() - 1;
    for (int i = 0; i < numVms; i++) {
       addEntry(aRegion);
    }
  }

  /**
   *  Performs puts/gets on entries in TestRegion
   */
  public static void HydraTask_doEntryOperations() {
    try {
       testInstance.doEntryOperations();
    } catch (Exception e) {
       Log.getLogWriter().info("doEntryOperations threw Exception " + e);
       throw new TestException("doEntryOperations caught Exception " + TestHelper.getStackTrace(e));
    }
}

  protected void doEntryOperations() {
    TestHelper.checkForEventError(PrTxBB.getBB());
    super.doEntryOperations(testRegion);
  }

  /**
   *  Performs random tx operations via functionExecution on each member in DS
   */
  public static void HydraTask_executeTx() {
    Function f = FunctionService.getFunction("parReg.tx.ExecuteTx");

    Cache myCache = CacheHelper.getCache();
    Set allRegions = myCache.rootRegions();
    for (Iterator it = allRegions.iterator(); it.hasNext();) {
      Region aRegion = (Region)it.next();
      testInstance.executeTx(aRegion, f);
    }
  }

  protected void executeTx(Region aRegion, Function f) {
    DistributedSystem ds = CacheHelper.getCache().getDistributedSystem();
    DistributedMember dm = ds.getDistributedMember();

    int numVms = TestConfig.getInstance().getTotalVMs() - 1;
    for (int i=0; i < numVms; i++) { 
      Set filter = new HashSet();
      filter.add(NameFactory.getObjectNameForCounter((long)i));
      Execution e = FunctionService.onRegion(aRegion).withArgs(dm.toString()).withFilter(filter);

      Log.getLogWriter().info("executing " + f.getId() + " with filter " + filter);
      ResultCollector rc = e.execute(f.getId());
      Log.getLogWriter().info("executed " + f.getId());

      List results = (List)rc.getResult();
      Log.getLogWriter().info("ResultCollector.getResult() = " + results);
  
    }
  }

  /**
   * Execute GetAllMembersInDS method
   */
  public static void HydraTask_executeGetAllMembersInDS() {
    Function f = FunctionService.getFunction("parReg.tx.GetAllMembersInDS");
    testInstance.executeGetAllMembersInDS(f);
  }

  protected void executeGetAllMembersInDS(Function f) {
    DistributedSystem ds = CacheHelper.getCache().getDistributedSystem();
    DistributedMember dm = ds.getDistributedMember();

    Execution e = FunctionService.onMembers(ds).withArgs(dm.toString());
    Log.getLogWriter().info("executing " + f.getId());

    ResultCollector rc = e.execute(f);
    Log.getLogWriter().info("executed " + f.getId());

    List results = (List)rc.getResult();
    Log.getLogWriter().info("ResultCollector.getResult() = " + results);

    StringBuffer s = new StringBuffer();
    s.append("ResultCollector : \n");
    ArrayList pidList = new ArrayList();
    for (Iterator it = results.iterator(); it.hasNext(); ) {
      SerializableDistributedMember sdm = (SerializableDistributedMember)it
          .next();
       s.append("   " + sdm.toString() + "\n");
       pidList.add(new Integer(sdm.getPid()));
    }
    // Save client pids on BB for later use as a filter (for CallbackResolver)
    PrTxBB.getBB().getSharedMap().put(PrTxBB.pidList, pidList);
    Log.getLogWriter().info(s.toString());
  }

  /**
   * Execute VerifyCustomPartitioningFunction on all PRs
   */
  public static void HydraTask_executeVerifyCustomPartitioningFunction() {
    Function f = FunctionService.getFunction("parReg.tx.VerifyCustomPartitioningFunction");

    Cache myCache = CacheHelper.getCache();
    Set allRegions = myCache.rootRegions();
    for (Iterator it = allRegions.iterator(); it.hasNext();) {
      PartitionedRegion pr = (PartitionedRegion)it.next();
      if (!(pr.getName().equalsIgnoreCase("routingRegion"))) {
         testInstance.executeVerifyCustomPartitioningFunction(pr, f);
      }
    }
  }

  protected void executeVerifyCustomPartitioningFunction(PartitionedRegion pr, Function f) {
    DistributedSystem ds = CacheHelper.getCache().getDistributedSystem();
    DistributedMember dm = ds.getDistributedMember();

    int numVms = TestConfig.getInstance().getTotalVMs() - 1;
    List pidList = (List)PrTxBB.getBB().getSharedMap().get(PrTxBB.pidList);
    for (int i=0; i < numVms; i++) { 
      Set filter = new HashSet();
      // The PartitionResolver filter is a Set of region entry keys.  
      // The product invokes the PartitionResolver to get the corresponding 
      // RoutingObjects.
      // The KEY_RESOLVER filter is a Set of ModRoutingObjects.
      // The CALLBACK_RESOLVER filter is a Set of PidRoutingObjects.
      switch (customPartitionMethod) {
        case PrTxPrms.CALLBACK_RESOLVER:
          Integer pid = (Integer)pidList.get(i);
          filter.add(new PidRoutingObject("callback", pid.intValue()));
          break;
        case PrTxPrms.KEY_RESOLVER:
          filter.add(new ModRoutingObject(NameFactory.getObjectNameForCounter((long)i)));
          break;
        case PrTxPrms.PARTITION_RESOLVER:
          filter.add(NameFactory.getObjectNameForCounter((long)i));
          break;
        default:  
          String s = "Test expects a customPartitionMethod based on PartitionResolver, KeyResolver or CallbackResolver";
          Log.getLogWriter().info(s);
          throw new HydraConfigException(s);
      }
      
      Execution e = FunctionService.onRegion(pr).withArgs(dm.toString()).withFilter(filter);
      Log.getLogWriter().info("executing " + f.getId());
  
      ResultCollector rc = e.execute(f);
      Log.getLogWriter().info("Finished executing " + f.getId());
  
      List results = (List)rc.getResult();
      Log.getLogWriter().info("ResultCollector.getResult() = " + results);
  
      StringBuffer s = new StringBuffer();
      s.append("ResultCollector : \n");
      for (Iterator it = results.iterator(); it.hasNext(); ) {
        ModResult result = (ModResult)it.next();
         // Everything should have mapped to the same hash code
         if (result.getHashList().size() > 1) {
            String err = "Keys in " + result.getDM().toString() + " mapped to more than one hash code " + result.getHashList() + "\n" + TestHelper.getStackTrace();
            Log.getLogWriter().info(err);
            throw new TestException(err);
         }
         s.append("   " + result.getDM().toString() + ": " + result.getNumKeys() + " entries with routingObjects (hashCodes) = " + result.getHashList() + "\n");
      }
      Log.getLogWriter().info(s.toString());
    }
  }

//===========================================================
// Override OperationsClient methods 
// (for implementing PartitionResolver in Key or Callback)
//===========================================================

  /* (non-Javadoc)
   * @see util.OperationsClient#getNewKey()
   */
  @Override
  protected Object getNewKey() {
    Object key = super.getNewKey();
    if (customPartitionMethod == PrTxPrms.KEY_RESOLVER) {
      key = new ModRoutingObject(key);
    }
    return key;
  }
  
  /* (non-Javadoc)
   * @see util.OperationsClient#getValueForKey(java.lang.Object)
   */
  @Override
  public BaseValueHolder getValueForKey(Object key) {
    if (key instanceof ModRoutingObject) {
      return super.getValueForKey(((ModRoutingObject)key).getKey());
    } else {
      return super.getValueForKey(key);
    }
  }
  
   /** Add a new entry to the given region.
    *
    *  @param aRegion The region to use for adding a new entry.
    */
   protected void addEntry(Region aRegion) {
      Object key = getNewKey();
      BaseValueHolder anObj = getValueForKey(key);

      Object callback = getCallback(key, createCallbackPrefix, ProcessMgr.getProcessId());
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use a create call
         Log.getLogWriter().info("addEntry: calling create for key " + key + ", object " + TestHelper.toString(anObj) + " cacheWriterParam is " + callback + ", region is " + aRegion.getFullPath());
         aRegion.create(key, anObj, callback);
         Log.getLogWriter().info("addEntry: done creating key " + key);
      } else { // use a put call
         Log.getLogWriter().info("addEntry: calling put for key " + key + ", object " + TestHelper.toString(anObj) + " callback is " + callback + ", region is " + aRegion.getFullPath());
         aRegion.put(key, anObj, callback);
         Log.getLogWriter().info("addEntry: done putting key " + key);
      }
      // Keep the PidRoutingObject for future operations to use
      if (customPartitionMethod == PrTxPrms.CALLBACK_RESOLVER) {
         routingRegion.put(key, callback);
      }
   }
       
   /** Add a new entry to the given region using the ConcurrentMap putIfAbsent method.
    *
    *  @param aRegion The region to use for adding a new entry.
    *
    *  Note:  Cannot be used with CALLBACK_RESOLVER (callbacks not yet supported).
    *         A TestException will be thrown in this case.
    */
   protected void putIfAbsent(Region aRegion, boolean logAddition) {
      Object key = getNewKey();
      BaseValueHolder anObj = getValueForKey(key);
 
      if (customPartitionMethod == PrTxPrms.CALLBACK_RESOLVER) {
        throw new TestException("putIfAbsent does not support callbacks.  This operation cannot be executed with customPartitionMethod.CALLBACK_RESOLVER");
      }

      if (logAddition) {
         Log.getLogWriter().info("putIfAbsent: calling putIfAbsent for key " + key + ", object " + TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath());
      }
      aRegion.putIfAbsent(key, anObj);
      Log.getLogWriter().info("putIfAbsent: done putting key " + key);
   }
       
   /** Invalidate an entry in the given region.
    *
    *  @param aRegion The region to use for invalidating an entry.
    *  @param isLocalInvalidate True if the invalidate should be local, false otherwise.
    */
   protected void invalidateEntry(Region aRegion, boolean isLocalInvalidate) {
      Object key = getExistingKey(aRegion);
      if (key == null) {
         return;
      }

      try {
         Object callback = getCallback(key, invalidateCallbackPrefix, ProcessMgr.getProcessId());
         if (isLocalInvalidate) { // do a local invalidate
            Log.getLogWriter().info("invalidateEntry: local invalidate for " + key + " callback is " + callback);
            aRegion.localInvalidate(key, callback);
            Log.getLogWriter().info("invalidateEntry: done with local invalidate for " + key);
         } else { // do a distributed invalidate
            Log.getLogWriter().info("invalidateEntry: invalidating key " + key + " callback is " + callback);
            aRegion.invalidate(key, callback);
            Log.getLogWriter().info("invalidateEntry: done invalidating key " + key);
         } 
      } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
   }
       
   /** Destroy an entry in the given region.
    *
    *  @param aRegion The region to use for destroying an entry.
    *  @param isLocalDestroy True if the destroy should be local, false otherwise.
    */
   protected void destroyEntry(Region aRegion, boolean isLocalDestroy) {
      Object key = getExistingKey(aRegion);
      if (key == null) {
         return;
      }

      try {
         Object callback = getCallback(key, destroyCallbackPrefix, ProcessMgr.getProcessId());
         if (isLocalDestroy) { // do a local destroy
            Log.getLogWriter().info("destroyEntry: local destroy for " + key + " callback is " + callback);
            aRegion.localDestroy(key, callback);
            Log.getLogWriter().info("destroyEntry: done with local destroy for " + key);
         } else { // do a distributed destroy
            Log.getLogWriter().info("destroyEntry: destroying key " + key + " callback is " + callback);
            aRegion.destroy(key, callback);
            Log.getLogWriter().info("destroyEntry: done destroying key " + key);
         }
      } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
   }
       
   /** Destroy an entry in the given region using the ConcurrentMap remove method.
    *
    *  @param aRegion The region to use for removing an entry.
    */
   protected void remove(Region aRegion) {
      Object key = getExistingKey(aRegion);
      if (key == null) {
         return;
      }

      try {
         Log.getLogWriter().info("remove: removing key " + key);
         aRegion.remove(key, aRegion.get(key));
         Log.getLogWriter().info("remove: done removing key " + key);
      } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
   }
       
   /** Update an existing entry in the given region. If there are
    *  no available keys in the region, then this is a noop.
    *
    *  @param aRegion The region to use for updating an entry.
    */
   protected void updateEntry(Region aRegion) {
      Object key = getExistingKey(aRegion);
      if (key == null) {
         return;
      }

      BaseValueHolder anObj = getUpdateObject(aRegion, key);
      Object callback = getCallback(key, updateCallbackPrefix, ProcessMgr.getProcessId());
      Log.getLogWriter().info("updateEntry: replacing key " + key + " with " +
      TestHelper.toString(anObj) + ", callback is " + callback);
      aRegion.put(key, anObj, callback);
      Log.getLogWriter().info("Done with call to put (update)");
   }

  @Override
  protected BaseValueHolder getUpdateObject(Region aRegion, Object key) {
    BaseValueHolder anObj = null;
    if (customPartitionMethod == PrTxPrms.KEY_RESOLVER) {
      anObj = super.getUpdateObject(aRegion, ((ModRoutingObject)key).getKey());
    } else {
      anObj = super.getUpdateObject(aRegion, key);
    }
    return anObj;
  }
  
   /** Replace an existing entry in the given region using the ConcurrentMap replace method.
    *  If there are no available keys in the region, then this is a noop.
    *
    *  @param aRegion The region to use for updating an entry.
    *
    *  ConcurrentMap methods currently do not support callbacks, so this operation may 
    *  not be used for customResolverMethod.CALLBACK_RESOLVER.  A TestException will 
    *  be thrown in this case.
    */
   protected void replace(Region aRegion) {
      if (customPartitionMethod == PrTxPrms.CALLBACK_RESOLVER) {
         throw new TestException("ConcurrentMap API replace does not support callbackArgs");
      }

      Object key = getExistingKey(aRegion);
      if (key == null) {
         return;
      }

      BaseValueHolder anObj = null;
      anObj = getUpdateObject(aRegion, key);
      Log.getLogWriter().info("replaceEntry: replacing key " + key + " with " + TestHelper.toString(anObj));
     
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use prevValue api
         aRegion.replace(key, aRegion.get(key), anObj);
      } else {
         aRegion.replace(key, anObj);
      }
      Log.getLogWriter().info("Done with call to replace");
   }
       
   /** Get an existing key in the given region if one is available,
    *  otherwise get a new key. 
    *
    *  @param aRegion The region to use for getting an entry.
    */
   protected void getKey(Region aRegion) {
      Object key = getExistingKey(aRegion);
      if (key == null) { // no existing keys; get a new key then
         return;
      }
      Object callback = getCallback(key, getCallbackPrefix, ProcessMgr.getProcessId());
      Object anObj = null;
      Log.getLogWriter().info("getKey: getting key " + key + ", callback is " + callback);
      anObj = aRegion.get(key, callback);
      Log.getLogWriter().info("getKey: got value for key " + key + ": " + TestHelper.toString(anObj));
   }
       
   /** Get a new key in the given region.
    *
    *  @param aRegion The region to use for getting an entry.
    */
   protected void getNewKey(Region aRegion) {
      Object key = getNewKey();
      Object callback = getCallback(key, getCallbackPrefix, ProcessMgr.getProcessId());
      int beforeSize = aRegion.size();
      Object anObj = null;
      Log.getLogWriter().info("getNewKey: getting new key " + key + ", callback is " + callback);
      anObj = aRegion.get(key, callback);
      Log.getLogWriter().info("getNewKey: done getting value for new key " + key + ": " + TestHelper.toString(anObj));
      if (customPartitionMethod == PrTxPrms.CALLBACK_RESOLVER) {
        routingRegion.put(key, callback);
      }
   }

  private Object getCallback(Object key, String prefix, int pid) {
     String sCallback = prefix + pid;
     if (customPartitionMethod == PrTxPrms.CALLBACK_RESOLVER) {
       Object o = routingRegion.get(key);
       if (o != null) {
         PidRoutingObject ro = (PidRoutingObject)routingRegion.get(key);
         return new PidRoutingObject(prefix, ro.getPid());
       } else {
         return new PidRoutingObject(prefix, pid);
       }
     }
     else return sCallback;
  }
}
