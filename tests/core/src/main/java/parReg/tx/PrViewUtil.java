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

import util.*;
import tx.*;
import tx.Operation;

import hydra.*;
import hydra.blackboard.*;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.*;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;

import java.util.*;

/** A class to contain methods useful for ParReg View transactions tests.
 * 
 *  It provides methods for creating and populating colocated regions.
 *  Registering functions (for function execution) and doing random operations
 *  on colocated entries as part of a transaction.
 */
public class PrViewUtil extends tx.TxViewUtil {

  /**
   *  Create the cache and Region.  
   */
  public synchronized static void HydraTask_createColocatedRegions() {
    if (txUtilInstance == null) {
      txUtilInstance = new PrViewUtil();
      try {
        ((PrViewUtil)txUtilInstance).createColocatedRegions();
        ((PrViewUtil)txUtilInstance).registerFunctions();
      } catch (Exception e) {
        Log.getLogWriter().info("initialize caught Exception " + e + ":" + e.getMessage());
        throw new TestException("initialize caught Exception " + TestHelper.getStackTrace(e));
      }
    }
  }

  /*
   * Creates cache and region (CacheHelper/RegionHelper)
   */
  public void createColocatedRegions() {
    super.initialize();
    // create cache/region (and connect to the DS)
    if (CacheHelper.getCache() == null) {
       Cache c = CacheHelper.createCache(ConfigPrms.getCacheConfig());
       CacheUtil.setCache(c);     // TxUtil and splitBrain/distIntegrityFD require this
       c.getCacheTransactionManager().setListener(TxPrms.getTxListener());
       ((CacheTransactionManager)c.getCacheTransactionManager()).setWriter(TxPrms.getTxWriter());
       if (TxBB.getUpdateStrategy().equalsIgnoreCase(TxPrms.USE_COPY_ON_READ)) {
          c.setCopyOnRead(true);
       }

       String regionConfig = ConfigPrms.getRegionConfig();
       AttributesFactory aFactory = RegionHelper.getAttributesFactory(regionConfig);
       RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
       String regionBase = rd.getRegionName();

       // override colocatedWith in the PartitionAttributes
       PartitionDescription pd = rd.getPartitionDescription();
       PartitionAttributesFactory prFactory = pd.getPartitionAttributesFactory();
       PartitionAttributes prAttrs = null;

       String colocatedWith = null;
       int numRegions = PrTxPrms.getNumColocatedRegions();
       for (int i = 0; i < numRegions; i++) {
          String regionName = regionBase + "_" + (i+1);
          if (i > 0) {
             colocatedWith = regionBase + "_" + i;
             prFactory.setColocatedWith(colocatedWith);
             prAttrs = prFactory.create();
             aFactory.setPartitionAttributes(prAttrs);
          }
          Region aRegion = RegionHelper.createRegion(regionName, aFactory);
       }
     }
  }

  /*
   * Creates cache and same regions as createColocatedRegions, but for edgeClients.
   * So, the regions will not be partitioned or colocatedWith other regions, but they
   * will have the same naming structure as the pr colocated server regions.
   */
  public synchronized static void HydraTask_createClientRegions() {
    if (txUtilInstance == null) {
      txUtilInstance = new PrViewUtil();
      ((PrViewUtil)txUtilInstance).createClientRegions();
      ((PrViewUtil)txUtilInstance).registerFunctions();
    }
  }

  public void createClientRegions() {
    super.initialize();
    // create cache/region (and connect to the DS)
    if (CacheHelper.getCache() == null) {
       Cache c = CacheHelper.createCache(ConfigPrms.getCacheConfig());
       CacheUtil.setCache(c);     // TxUtil and splitBrain/distIntegrityFD require this
       c.getCacheTransactionManager().setListener(TxPrms.getTxListener());
       ((CacheTransactionManager)c.getCacheTransactionManager()).setWriter(TxPrms.getTxWriter());
       if (TxBB.getUpdateStrategy().equalsIgnoreCase(TxPrms.USE_COPY_ON_READ)) {
          c.setCopyOnRead(true);
       }

       String regionConfig = ConfigPrms.getRegionConfig();
       AttributesFactory aFactory = RegionHelper.getAttributesFactory(regionConfig);
       RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
       String regionBase = rd.getRegionName();

       int numRegions = PrTxPrms.getNumColocatedRegions();
       for (int i = 0; i < numRegions; i++) {
          String regionName = regionBase + "_" + (i+1);
          Region aRegion = RegionHelper.createRegion(regionName, aFactory);

          // edge clients register interest in ALL_KEYS
          aRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
          Log.getLogWriter().info("registered interest in ALL_KEYS for " + regionName);
       }
     }
  }
 /*
  *  Creates initial set of entries across colocated regions
  *  (non-transactional).
  */
  public static void HydraTask_populateRegions() {
     ((PrViewUtil)txUtilInstance).populateRegions();
  }
 
  public void populateRegions() {
     // prime the PartitionRegion by getting at least one entry in each bucket
     Set regions = CacheUtil.getCache().rootRegions();
     Region sampleRegion = (Region)regions.iterator().next();
     int numBuckets = sampleRegion.getAttributes().getPartitionAttributes().getTotalNumBuckets() * TestHelper.getNumVMs();
     
     for (int i = 0; i < numBuckets; i++) {
        Object key = NameFactory.getNextPositiveObjectName();
  
        // create this same key in each region 
        Set rootRegions = CacheUtil.getCache().rootRegions();
        for (Iterator it = rootRegions.iterator(); it.hasNext(); ) {
           Region aRegion = (Region)it.next();
           BaseValueHolder vh = new ValueHolder(key, randomValues, new Integer(modValInitializer++));
           String callback = createCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();
           Log.getLogWriter().info("populateRegion: calling create for key " + key + ", object " + TestHelper.toString(vh) + ", region is " + aRegion.getFullPath());
           aRegion.create(key, vh, callback);
           Log.getLogWriter().info("populateRegion: done creating key " + key);
        }
     }
  }

  /** Debug method to see what keys are on each VM at start of test
   *
   */
  public static void HydraTask_dumpLocalKeys() {
     ((PrViewUtil)txUtilInstance).dumpLocalKeys();
  }

  protected void dumpLocalKeys() {
     StringBuffer aStr = new StringBuffer();
     DistributedMember dm = DistributedSystemHelper.getDistributedSystem().getDistributedMember();

     aStr.append("Keys hosted as primary " + dm.toString() + " by region\n");
     Set rootRegions = CacheUtil.getCache().rootRegions();
     for (Iterator it = rootRegions.iterator(); it.hasNext(); ) {
        Region aRegion = (Region)it.next();
        Region localRegion = PartitionRegionHelper.getLocalData(aRegion);
        Set keySet = localRegion.keySet();
        HashSet primaryKeySet = new HashSet();
        for (Iterator kit = keySet.iterator(); kit.hasNext();) {
          Object key = kit.next();
          DistributedMember primary = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, key);
          if (primary.equals(dm)) {
            primaryKeySet.add(key);
          }
        }
        aStr.append("   " + aRegion.getName() + ": " + primaryKeySet + "\n");
     }
     Log.getLogWriter().info(aStr.toString());
  }

  protected void registerFunctions() {
    Function f = new GetAllMembersInDS();
    FunctionService.registerFunction(f);

    f = new GetNewKey();
    FunctionService.registerFunction(f);

    f = new GetKeySet();
    FunctionService.registerFunction(f);

    f = new GetValueForKey();
    FunctionService.registerFunction(f);

    f = new SetupRemoteTestCallbacks();
    FunctionService.registerFunction(f);

    Log.getLogWriter().info("Registered functions: " + FunctionService.getRegisteredFunctions());
  }

  /** Do random operations on random regions using the hydra parameter
   *  TxPrms.operations as the list of available operations and
   *  TxPrms.numOps - the number of operations to execute in a single tx
   *
   *  @param allowGetOperations - to prevent proxy clients from attempting
   *                              gets (since they have no storage this always
   *                              invokes the loader and oldValue is always
   *                              reported as null).
   */
  public static OpList doOperations(boolean allowGetOperations) {
     Vector operations = TestConfig.tab().vecAt(TxPrms.operations);
     int numOps = TestConfig.tab().intAt(TxPrms.numOps);
     return ((PrViewUtil)txUtilInstance).doOperations(operations, numOps, allowGetOperations);
  }

  /** Do random operations on random regions.
   *  
   *  @param operations - a Vector of operations to choose from.
   *  @param numOperationsToDo - the number of operations to execute.
   *  @param allowGetOperations - if false, replace entry-get ops with entry-create
   *
   *  @returns An instance of OpList, which is a list of operations that
   *           were executed.
   */
  public OpList doOperations(Vector operations, int numOperationsToDo, boolean allowGetOperations) {
     Log.getLogWriter().info("Executing " + numOperationsToDo + " random operations in tx");

     final long TIME_LIMIT_MS = 60000;  
        // limit on how long this method will try to do UNSUCCESSFUL operations; 
        // for example, if the test is configured such that it destroys
        // entries but does not create any, eventually no entries will be left
        // to destroy and this method will be unable to fulfill its mission
        // to execute numOperationsToDo operations; This is like a consistency
        // check for test configuration problems
     long timeOfLastOp = System.currentTimeMillis();
     OpList opList = new OpList();

     // initialize keySet (on BB) for this round of execution
     // getNewKey and getRandomKey rely on this thread specific keySet
     initializeKeySet(PrTxPrms.useLocalKeySet());
  
     int numOpsCompleted = 0;
     while (numOpsCompleted < numOperationsToDo) {
        tx.Operation op = null;
  
        // choose a random operation, forcing a region create if there are no regions
        String operation = (String)(operations.get(TestConfig.tab().getRandGen().nextInt(0, operations.size()-1)));
        if (!operation.equals(tx.Operation.CACHE_CLOSE) && !operation.equals(tx.Operation.REGION_CREATE)) {
           // operation requires a choosing a random region
           Object[] rootRegionArr = CacheUtil.getCache().rootRegions().toArray();
           if (rootRegionArr.length == 0) { // no regions available; force a create region op
              if (operations.indexOf(tx.Operation.REGION_CREATE) < 0) // create not specified
                 throw new TestException("No regions are available and no create region operation is specified");
              Log.getLogWriter().info("In doOperations, forcing region create because no regions are present");
              operation = tx.Operation.REGION_CREATE;
           }
        }
  
        // override get operations with create (for proxy view clients)
        if (operation.startsWith("entry-get") && !allowGetOperations) {
           operation = tx.Operation.ENTRY_CREATE;
        }
  
        Log.getLogWriter().info("Operation is " + operation);
    
        if (operation.equalsIgnoreCase(tx.Operation.ENTRY_CREATE)) {
           op = createEntry(getRandomRegion(true), false);
        } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_UPDATE)) {
           Region aRegion = getRandomRegion(true);
           op = updateEntry(aRegion, getRandomKey(aRegion));
        } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_PUTALL)) {
           List<Operation> putAllOps = putAll(getRandomRegion(true));
           for (Operation o : putAllOps) {
             opList.add(o);
             updateKeySet(o);
             numOpsCompleted++;
           }
           timeOfLastOp = System.currentTimeMillis();
           continue;
        } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_DESTROY)) {
           Region aRegion = getRandomRegion(true);
           op = destroyEntry(false, aRegion, getRandomKey(aRegion), false);
        } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_LOCAL_DESTROY)) {
           Region aRegion = getRandomRegionNoReplication(true);
           if (aRegion != null)
              op = destroyEntry(true, aRegion, getRandomKey(aRegion), false);
        } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_INVAL)) {
           Region aRegion = getRandomRegion(true);
           op = invalEntry(false, aRegion, getRandomKey(aRegion), false);
        } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_LOCAL_INVAL)) {
           Region aRegion = getRandomRegionNoReplication(true);
           if (aRegion != null)
              op = invalEntry(true, aRegion, getRandomKey(aRegion), false);
        } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_GET_NEW_KEY)) {
           Region aRegion = getRandomRegion(true);
           op = getEntryWithNewKey(aRegion);
        } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_GET_EXIST_KEY)) {
           Region aRegion = getRandomRegion(true);
           op = getWithExistingKey(aRegion);
        } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_GET_PREV_KEY)) {
           Region aRegion = getRandomRegion(true);
           op = getEntryWithPreviousKey(aRegion);
        } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_GETALL)) {
           op = getAll(getRandomRegion(true));
        } else if (operation.equalsIgnoreCase(tx.Operation.REGION_CREATE)) {
           op = createRegion();
        } else if (operation.equalsIgnoreCase(tx.Operation.REGION_DESTROY)) {
           op = destroyRegion(false, getRandomRegion(true));
        } else if (operation.equalsIgnoreCase(tx.Operation.REGION_LOCAL_DESTROY)) {
           Region aRegion = getRandomRegionNoReplication(true);
           if (aRegion != null)
              op = destroyRegion(true, aRegion);
        } else if (operation.equalsIgnoreCase(tx.Operation.REGION_INVAL)) {
           Region aRegion = getRandomRegion(true);
           op = invalRegion(false, getRandomRegion(true));
        } else if (operation.equalsIgnoreCase(tx.Operation.REGION_LOCAL_INVAL)) {
           Region aRegion = getRandomRegionNoReplication(true);
           if (aRegion != null)
              op = invalRegion(true, aRegion);
        } else if (operation.equalsIgnoreCase(tx.Operation.CACHE_CLOSE)) {
           op = closeCache();
        } else { // unknown operation
          throw new TestException("Unknown operation " + operation);
        }
        if (op == null) { // op could be null if, for example, we tried to do an op on an empty region
           // make sure we don't continually try operations that cannot be successful; see
           // comments on TIME_LIMIT_MS above
           if (System.currentTimeMillis() - timeOfLastOp > TIME_LIMIT_MS) {
              throw new TestException("Could not execute a successful operation in " + TIME_LIMIT_MS +
                                      " millis; possible test config problem");
           }
        } else {
           opList.add(op);
           updateKeySet(op);
           numOpsCompleted++;
           timeOfLastOp = System.currentTimeMillis();
        }
     } 
     Log.getLogWriter().info("Completed execution of " + opList);
     return opList;
  }

  /** Update (per region) keySet 
   *  Region specific keySets are maintained through each invocation
   *  of doOperations().  They are initialized at the beginning, but keys
   *  need to be removed on entryDestroy and added on entryCreate.
   *  We don't have a test hook to look at the TxState (vs. committed state).
   */
   protected void updateKeySet(tx.Operation op) {
     String regionName = op.getRegionName();
     Object key = op.getKey();
 
     String mapKey = PrTxBB.keySet + regionName;
     KeySetResult keySetResult = (KeySetResult)PrTxBB.getBB().getSharedMap().get(mapKey);
     List keyList = keySetResult.getKeySet();
     
     if (op.isEntryCreate()) {   // add to keySet
        keyList.add(key);
     } else if (op.isEntryDestroy()) {  // remote from keySet
        keyList.remove(key);
     }

     PrTxBB.getBB().getSharedMap().put(mapKey, keySetResult);
   }

  /** Initialize remote keySet.
   *
   *  Writes a KeySetResult (with DistributedMember and keySet) to PrTxBB.
   *  getRandomKey() relies upon this (to select colocated entries on
   *  a single remote VM).
   *  getNewKey() also relies on this (so it can determine the correct 
   *  hashCode required for a new entry on the remote VM).
   */
  protected void initializeKeySet(boolean useLocalKeySet) {

    DistributedMember targetDM = null;

    // For Client/Server variants (parRegBridgeIntegrity*.conf), we want to ensure we
    // select a targetDM which is NOT the delegate
    // initialization task writes port <=> DM mapping to BB
    DistributedMember excludeDM = null;
    PoolImpl pool = (PoolImpl)PoolManager.find("brloader");
    if (pool != null) {
      ServerLocation delegate  = pool.getNextOpServerLocation();
      String delegateHost = delegate.getHostName();
      int dot = delegateHost.indexOf(".");
      if (dot > 0) {
        delegateHost = delegateHost.substring(0, dot);
      }
      int delegatePort = delegate.getPort();
      String mappingKey = delegateHost + ":" + delegatePort;
      excludeDM = (DistributedMember)TxBB.getBB().getSharedMap().get(mappingKey);
      Log.getLogWriter().info("Retrieved port to dm mapping from BB for excludeDM: " + mappingKey + ":" + excludeDM);
    }

    // For the local VM, we can set the targetDM now.
    // For remote VMs, we'll get the target DM from the first call to getKeySet
    if (useLocalKeySet) {
       targetDM = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
    }

    // get the keySet for each region (either locally or on a single remote vm)
    Set rootRegions = CacheUtil.getCache().rootRegions();
    for (Iterator it = rootRegions.iterator(); it.hasNext(); ) {
       Region aRegion = (Region)it.next();

       // Set a keySet (specific to a random member) for this round of execution
       // getRandomKey and getNewKeyForRegion depend on this being set in the BB
       KeySetResult keySetResult = getKeySet(aRegion, targetDM, excludeDM);
       String mapKey = PrTxBB.keySet + aRegion.getFullPath();
       PrTxBB.getBB().getSharedMap().put(mapKey, keySetResult);
       targetDM = keySetResult.getDistributedMember();
       PrTxBB.getBB().getSharedMap().put(PrTxBB.targetDM, targetDM);
       List keyList = (List)keySetResult.getKeySet();
       Object[] keySet = keyList.toArray();
       Log.getLogWriter().info("KeySet for " + targetDM + "  = " + keyList);
    }
  }

  protected KeySetResult getKeySet(Region aRegion, DistributedMember targetDM) {
    return getKeySet(aRegion, targetDM, null);
  }

  protected KeySetResult getKeySet(Region aRegion, DistributedMember targetDM, DistributedMember excludeDM) {
    KeySetResult ksResult = null;

    DistributedMember localDM = DistributedSystemHelper.getDistributedSystem().getDistributedMember();

    Function f = FunctionService.getFunction("parReg.tx.GetKeySet");
    Execution e = FunctionService.onRegion(aRegion).withArgs(localDM.toString());

    // suspend the transaction (function execution targeting multiple nodes is not allowed within tx)
    TXStateInterface txState = TxHelper.internalSuspend();

    Log.getLogWriter().info("executing " + f.getId() + " on region " + aRegion.getName());
    ResultCollector rc = e.execute(f);
    Log.getLogWriter().info("executed " + f.getId());

    // resume transaction
    TxHelper.internalResume(txState);

    List results = (List)rc.getResult();
    DistributedMember remoteDM = null;
    boolean keySetAvailable = false;
    for (Iterator rit = results.iterator(); rit.hasNext(); ) {
       ksResult = (KeySetResult)rit.next(); 
       remoteDM = ksResult.getDistributedMember();

       Log.getLogWriter().fine("getKeySet DM selection: targetDM = " + targetDM + ", localDM = " + localDM + ", excludeDM = " + excludeDM + ", remoteDM = " + remoteDM);
       // targetDM is null on the first invocation for a remote VM 
       // it is set to the local DM if we want local keySets
       if (targetDM == null) {               // looking for a remote VM
          if (localDM.equals(remoteDM)) {
            Log.getLogWriter().fine("skipping localDM " + localDM);
            // skip this one
          } else if (excludeDM == null) {
             Log.getLogWriter().fine("excludeDM == null, taking first remote response from " + remoteDM);
             targetDM = remoteDM;
          } else if (!(excludeDM.equals(remoteDM))) {
             Log.getLogWriter().fine("excludeDM == null, targetDM = first DM which is not excludeDM(" + excludeDM + ") " + remoteDM);
             targetDM = remoteDM;
          }
       } 

       if (targetDM != null) {
          if (remoteDM.equals(targetDM)) {
             // we've got a keySet for the target dataStore
             keySetAvailable = true;
             break;
          }
       } 
    }

    if (!keySetAvailable) {
       ksResult = null;
       throw new TestException("Test issue with getNewKey -- no keySet found");
    } else {
       Log.getLogWriter().info("Returning keySet from member " + targetDM);
    }
    return ksResult;
  }

  protected List executeGetAllMembersInDS() {
    DistributedSystem ds = CacheUtil.getCache().getDistributedSystem();
    DistributedMember dm = ds.getDistributedMember();

    Function f = FunctionService.getFunction("parReg.tx.GetAllMembersInDS");
    Execution e = FunctionService.onMembers(ds).withArgs(dm.toString());

    Log.getLogWriter().info("executing " + f.getId());
    ResultCollector rc = e.execute(f);
    Log.getLogWriter().info("executed " + f.getId());

    List results = (List)rc.getResult(); 
    Log.getLogWriter().info("ResultCollector.getResult() = " + results);

    StringBuffer s = new StringBuffer();
    s.append("ResultCollector : \n");
    ArrayList dmList = new ArrayList();
    for (Iterator it = results.iterator(); it.hasNext(); ) {
       SerializableDistributedMember sdm = (SerializableDistributedMember)it
          .next();
       s.append("   " + sdm.toString() + "\n");
       dmList.add(sdm.getDistributedMember());
    }
    Log.getLogWriter().info(s.toString());
    return (dmList);
  }

  public static Object getValueForKey(Region aRegion, String key) {
    Object val = null;

    // suspend the transaction (function execution is not allowed within tx)
    TXStateInterface txState = TxHelper.internalSuspend();

    Function f = FunctionService.getFunction("parReg.tx.GetValueForKey");
    Execution e = FunctionService.onRegion(aRegion).withArgs(key);

    Log.getLogWriter().info("executing " + f.getId() + " on region " + aRegion.getName() + " and key " + key);
    ResultCollector rc = e.execute(f);
    Log.getLogWriter().info("executed " + f.getId());

    List results = (List)rc.getResult();
    for (Iterator rit = results.iterator(); rit.hasNext();) {
      Object testVal = rit.next();
      if (testVal != null) {
        val = testVal;
        break;
      }
    }

    // resume transaction
    TxHelper.internalResume(txState);

    Log.getLogWriter().info("Returning value " + val + " for key " + key);
    return val;
  }

  /** Get a random key from the given region, excluding the key specified
   *  by excludeKey. If no keys qualify in the region, return null.
   *
   *  @param aRegion - The region to get the key from.
   *  @param excludeKey - The region to get the key from.
   *
   *  @returns A key from aRegion, or null.
   */
  public Object getRandomKey(Region aRegion, Object excludeKey) {
     // getting a random key can put all the keys into the remembered
     // set, so suspend the tx if suspendResume is true, then resume it
     // before leaving this method so the test can test according to
     // its strategy without the test framework getting in the way.
     if (suspendResume) {
        txState.set(TxHelper.internalSuspend());
     }

     if (aRegion == null) {
        return null;
     }

     // get thread specific keySet from BB
     String mapKey = PrTxBB.keySet + aRegion.getFullPath();
     KeySetResult keySetResult = (KeySetResult)PrTxBB.getBB().getSharedMap().get(mapKey);
     List keyList = (List)keySetResult.getKeySet();
     Object[] keyArr = keyList.toArray();

     if (keyArr.length == 0) {
        Log.getLogWriter().info("Could not get a random key from " + aRegion.getFullPath() + " because the region has no keys");
        return null;
     }
     int randInt = TestConfig.tab().getRandGen().nextInt(0, keyArr.length-1);
     Object key = keyArr[randInt];
     if (key.equals(excludeKey)) { // get another key
        if (keyArr.length == 1) { // there are no other keys
           return null;
        }
        randInt++; // go to the next key
        if (randInt == keyArr.length)
           randInt = 0;
        key = keyArr[randInt];
     }
     if (suspendResume) {
        TxHelper.internalResume((TXStateInterface)txState.get());
     }
     return key;
  }


  /** Gets a value in aRegion with a random key (local to the tx vm) 
   *  that was previously used in the test.
   * 
   *  @param aRegion The region to get from.
   *
   *  @return An instance of Operation describing the get operation.
   */
  public tx.Operation getEntryWithPreviousKey(Region aRegion) {
     // Unlike tx.TxUtil (which uses the entire NameFactory PositiveCounter
     // namespace, this method is restricted to known keys in this VM
     Object key = getRandomKey(aRegion);
     try {
        Object oldValue = getValueInVM(aRegion, key);
        if (oldValue instanceof BaseValueHolder)
           oldValue = ((BaseValueHolder)oldValue).modVal;
        Log.getLogWriter().info("getEntryWithPreviousKey: getting value for key " + key +
               " in region " + aRegion.getFullPath());
        BaseValueHolder vh = (BaseValueHolder)(aRegion.get(key));
        Log.getLogWriter().info("getEntryWithPreviousKey: got value for key " + key + ": " + vh +
               " in region " + aRegion.getFullPath());
        if (vh == null)
           return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_GET_PREV_KEY, oldValue, null);
        else
           return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_GET_PREV_KEY, oldValue, vh.modVal);
     } catch (RegionDestroyedException e) {
        if (isSerialExecution)
           throw e;
        // if concurrent, this is OK
        return null;
     } catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     } catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     }
  }
  
  /** Gets a new (never before seen) key.
   *  Invoked by createEntry and getEntryWithNewKey (TxUtil)
   * 
   *  @return the next key (with hashCode) for the target VM
   */
/*
  public Object getNewKey(Region aRegion) {
    // getRandomKey() works off the keySet stored in PrTxBB
    // this keySet is initialized for either a local or remote vm
    // based on PrTxPrms.useLocalKeySet
    Object sampleKey = getRandomKey(aRegion);
    DistributedMember sampleDM = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, sampleKey);
    Log.getLogWriter().info("Looking for a new key for = " + sampleDM + " colocated with " + sampleKey);
 
    // Use this to determine a new key for the local VM (with same hashCode)
    Object key = null;
    do {
      // take a look at the routing code for the next potential key
      Object pKey = NameFactory.getNextPositiveObjectName();
      DistributedMember potentialDM = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, pKey);
      if (sampleDM.equals(potentialDM)) {
         // we've found it!
         key = pKey;
         Log.getLogWriter().info("Found " + key + " for DM " + potentialDM);
      }
    } while (key == null);
    return key;
  }
*/


  /** Gets a new (never before seen) key.
   *  Invoked by createEntry and getEntryWithNewKey (TxUtil)
   *
   *  @return the next key (with hashCode) for the target VM
   */
  public Object getNewKey(Region aRegion) {

    // suspend the transaction (function execution is not allowed within tx)
    TXStateInterface txState = TxHelper.internalSuspend();

    Object sampleKey = getRandomKey(aRegion);
    Object newKey = null;

    Log.getLogWriter().info("Looking for a new key colocated with " + sampleKey);

    DistributedMember localDM = DistributedSystemHelper.getDistributedSystem().getDistributedMember();

    Function f = FunctionService.getFunction("parReg.tx.GetNewKey");
    Set filter = new HashSet();
    filter.add(sampleKey);
    Execution e = FunctionService.onRegion(aRegion).withArgs(localDM.toString()).withFilter(filter);

    Log.getLogWriter().info("executing " + f.getId() + " on region " + aRegion.getName() + " with filter " + filter);
    ResultCollector rc = e.execute(f);
    Log.getLogWriter().info("executed " + f.getId());

    List results = (List)rc.getResult();
    for (Iterator rit = results.iterator(); rit.hasNext();) {
      Object key = rit.next();
      if (key != null) {
        newKey = key;
        break;
      }
    }
    Log.getLogWriter().info("Found new key " + newKey + " from same member as sampleKey " + sampleKey);

    // resume transaction
    TxHelper.internalResume(txState);
    return newKey;
  }
}
