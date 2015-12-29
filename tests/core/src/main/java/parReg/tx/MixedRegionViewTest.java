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

import tx.*;
import util.*;

import hydra.*;
import hydra.blackboard.*;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.*;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import java.util.*;

/**
 * Serial execution test with client1 executing TX across Partition and 
 * Replicated Regions.  Tx thread and non-tx threads verify their view of the 
 * cache before and after commit.  Event counters (for both Cache and Tx Writers
 * and Listeners are also verified).
 */

public class MixedRegionViewTest {

  // Singleton instance in this VM
  static public MixedRegionViewTest testInstance = null;

  // instance fields
  protected GsRandom rng;
  protected boolean txInProgress = false;
  protected boolean isProxy = false;
  protected Integer myVmPid;
  protected boolean initializeTxInfo = true;
  protected boolean checkEventCounters;
  protected boolean useFunctionExecution;

  // because hydra executes each task in a new HydraThread, the transaction
  // manager doesn't associate our commits with our begins (its not truly
  // the same thread.  We can use suspend to get the current TXState (after
  // begin() ... and then upon re-entering to either commit or rollback
  // re-instate that context.
  protected TXStateInterface txContext = null;

  public RandomValues randomValues = null; // for random object creation
  protected int maxKeys;            // initial object count (per region)
  public int modValInitializer=0;          // track ValueHolder.modVal
  protected boolean isSerialExecution; 
  protected HydraThreadLocal txState = new HydraThreadLocal();
  
  // static fields
  public static final int NO_REPLICATION_RESTRICTION = 0;
  public static final int USE_REPLICATED_REGION = 1;
  public static final int USE_PARTITIONED_REGION = 2;
  
  // String prefixes for event callback object
  protected static final String createCallbackPrefix = "Create event originated in pid ";
  protected static final String updateCallbackPrefix = "Update event originated in pid ";
  protected static final String invalidateCallbackPrefix = "Invalidate event originated in pid ";
  protected static final String destroyCallbackPrefix = "Destroy event originated in pid ";    
  protected static final String regionInvalidateCallbackPrefix = "Region invalidate event originated in pid ";
  protected static final String regionDestroyCallbackPrefix = "Region destroy event originated in pid ";
  
  protected static final String memberIdString = " memberId=";
  
  static int numPRsWithList = 1;  // default case is cache_content (primary only)
  static boolean useLocalKeySet;

// ======================================================================== 
// initialization
// ======================================================================== 
  public synchronized void initialize() {
    // establish our randomNumberGenerator (for commit vs. rollback)
    this.rng = TestConfig.tab().getRandGen();

    randomValues = new RandomValues();   
    isSerialExecution = TestConfig.tab().booleanAt(hydra.Prms.serialExecution);
    if (isSerialExecution) {
      Log.getLogWriter().info("Test is running in SERIAL_EXECUTION mode");
    }

    useFunctionExecution = PrTxPrms.useFunctionExecution();
    Log.getLogWriter().info("useFunctionExecution = " + useFunctionExecution);

    // cache this VM PID
    this.myVmPid = new Integer(RemoteTestModule.getMyPid());

    // cache boolean indicating whether or not to track cache & txListener events
    // based for now on whether or not a cacheListener is configured.
    checkEventCounters = TxPrms.checkEventCounters();
    Log.getLogWriter().info("checkEventCounters = " + checkEventCounters);
  
    // useLocalKeySet Prms sets expectation for isRemote events
    useLocalKeySet = PrTxPrms.useLocalKeySet();
    Log.getLogWriter().info("useLocalKeySet = " + useLocalKeySet);
  }

  public synchronized void initialize(String regionConfig) {
    // initialize region config independent fields
    initialize(); 

    // establish numPRsWithList for checkEventCounters
    //   cache_content = 1 (primary)
    //   gemfire.BucketRegion.alwaysFireLocalListeners = redundantCopies + 1
    StringBuffer aStr = new StringBuffer();
    boolean alwaysFireLocalListeners = Boolean.getBoolean("gemfire.BucketRegion.alwaysFireLocalListeners");
    aStr.append("alwaysFireLocalListeners= " + alwaysFireLocalListeners + " ");
    PrTxBB.getBB().getSharedMap().put(PrTxBB.alwaysFireLocalListeners, new Boolean(alwaysFireLocalListeners));

    RegionAttributes ratts = RegionHelper.getRegionAttributes(regionConfig);
    PartitionAttributes patts = ratts.getPartitionAttributes();
    int redundantCopies = patts.getRedundantCopies();
    aStr.append("redundantCopies = " + redundantCopies + " ");
    PrTxBB.getBB().getSharedMap().put(PrTxBB.redundantCopies, new Integer(redundantCopies));
    if (alwaysFireLocalListeners) {   
      numPRsWithList = redundantCopies + 1;   // primary + copies
      aStr.append("calculated numPRsWithListener = " + numPRsWithList + " ");
    }
    Log.getLogWriter().info(aStr.toString());
  }

  public static MixedRegionViewTest getTestInstance() {
    return testInstance;
  }

  /** Hydra task to create a forest of replicated regions.
   *  The HydraTask_createPartitionedRegions must be invoked prior to this method.
   */
  public synchronized static void HydraTask_createReplicatedRegions() {
    if (testInstance == null) {
      throw new TestException("HydraTask_createReplicatedRegions: createPartitionedRegions must be invoked prior to this method");
    }

    if (CacheUtil.getCache() != null) {
      testInstance.createRegionHierarchy(ConfigPrms.getRegionConfig());
    } else {
      throw new TestException("HydraTask_createReplicatedRegions: createPartitionedRegions must be invoked prior to this method");
    }
  }

  /** 
   *  Create a forest of regions based on numRootRegions,
   *  numSubRegions, regionDepth
   */
  public void createRegionHierarchy(String regionConfig) {
  
     int numRoots = TestConfig.tab().intAt(TxPrms.numRootRegions);
     int breadth = TestConfig.tab().intAt(TxPrms.numSubRegions);
     int depth = TestConfig.tab().intAt(TxPrms.regionDepth);
  
     Region r = null;
     for (int i=0; i<numRoots; i++) {
        String rootName = "root" + (i+1);
        Region rootRegion = RegionHelper.createRegion(rootName, regionConfig);
        Log.getLogWriter().info("Created root region " + rootName);
        createEntries( rootRegion );
        createSubRegions(rootRegion, breadth, depth, "Region");
     }
  }
  
  /** 
   *  Create the subregion hierarchy of a root. 
   */
  private void createSubRegions( Region r, int numChildren, int levelsLeft, String parentName) {
     String currentName;
     String regionConfig = ConfigPrms.getRegionConfig();
     for (int i=1; i<=numChildren; i++) {
        currentName = parentName + "-" + i;
        Region child = null;
        try {
           RegionAttributes regionAttrs = RegionHelper.getRegionAttributes(regionConfig);
           this.isProxy = (regionAttrs.getDataPolicy().isEmpty());   // cannot validate RR with DataPolicy.EMPTY

           child = r.createSubregion(currentName, regionAttrs);
           Log.getLogWriter().info("Created subregion " + TestHelper.regionToString(child, true));
        } catch (RegionExistsException e) {
           child = r.getSubregion(currentName);
           Log.getLogWriter().info("Got subregion " + TestHelper.regionToString(child, true));
        } catch (TimeoutException e) {
           throw new TestException(TestHelper.getStackTrace(e)); 
        }
        createEntries( child );
        if (levelsLeft > 1)
           createSubRegions(child, numChildren, levelsLeft-1, currentName);
     }
  }
  
  /**
   *  Create TxPrms.maxKeys entries in the given region
   *  Initializes ValueHolder.modVal sequentially 1-n 
   *  across all regions
   */
  private void createEntries(Region aRegion) {
    this.maxKeys = TestConfig.tab().intAt(TxPrms.maxKeys, 10);
    long startKey = 0;
    for (int i=0; i < maxKeys; i++) {
      String key = NameFactory.getObjectNameForCounter(startKey + i);
      NameBB.getBB().getSharedCounters().setIfLarger(NameBB.POSITIVE_NAME_COUNTER, startKey + i);
      Object val = new ValueHolder( key, randomValues, new Integer(modValInitializer));
      modValInitializer++;
      String callback = createCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();

      try {
        aRegion.create( key, val, callback );
      } catch (RegionDestroyedException e) {
        if (isSerialExecution) // not expected in serial tests
           throw e;
        Log.getLogWriter().info("Created " + i + " keys in " + aRegion.getFullPath() + 
               " before getting " + e + "; continuing test");
        break;
      } catch (EntryExistsException e) {
        // we received this entry via distribution (created by another VM)
        Log.getLogWriter().fine("Created via distribution in region <" + aRegion.getFullPath() + "> " + key + " = " + val.toString());
        continue;
      } 
//      catch (CacheException ce) {
//        throw new TestException("Cannot create key " + key + " CacheException(" + ce + ")");
//      }
      Log.getLogWriter().fine("Created in region <" + aRegion.getFullPath() + "> " + key + " = " + val.toString());
    }
  }

  /**
   *  Create the cache and Partitioned Regions.
   */
  public synchronized static void HydraTask_createPartitionedRegions() {
    if (testInstance == null) {
      testInstance = new MixedRegionViewTest();
      try {
        testInstance.createPartitionedRegions();
        testInstance.initialize(ConfigPrms.getRegionConfig());  // regions must exist prior to this initialization
        testInstance.registerFunctions();
      } catch (Exception e) {
        Log.getLogWriter().info("HydraTask_createPartitionedRegions caught Exception " + e + ":" + e.getMessage());
        throw new TestException("HydraTask_createPartitionedRegions caught Exception " + TestHelper.getStackTrace(e));
      }
    }
  }

  /*
   * Creates cache and region (CacheHelper/RegionHelper)
   */
  public void createPartitionedRegions() {
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
  *  Creates initial set of entries across colocated regions
  *  (non-transactional).
  */
  public static void HydraTask_populateRegions() {
     testInstance.populateRegions();
  }
 
  protected void populateRegions() {
     // prime the PartitionRegion by getting at least one entry in each bucket
     Set regions = CacheUtil.getCache().rootRegions();
     Region sampleRegion = null;
     for (Iterator it = regions.iterator(); it.hasNext(); ) {
       Region sRegion = (Region)it.next();
       if (PartitionRegionHelper.isPartitionedRegion(sRegion)) {
         sampleRegion = sRegion;
         break;
       }
     }
     if (sampleRegion == null) {
       throw new TestException("There are no PartitionedRegions present.  Check test config");
     }
     int numBuckets = sampleRegion.getAttributes().getPartitionAttributes().getTotalNumBuckets() * TestHelper.getNumVMs();
     
     for (int i = 0; i < numBuckets; i++) {
        Object key = NameFactory.getNextPositiveObjectName();
  
        // create this same key in each region 
        Set rootRegions = CacheUtil.getCache().rootRegions();
        for (Iterator it = rootRegions.iterator(); it.hasNext(); ) {
           Region aRegion = (Region)it.next();
           // skip any replicated regions
           if (PartitionRegionHelper.isPartitionedRegion(aRegion)) {
             BaseValueHolder vh = new ValueHolder(key, randomValues, new Integer(modValInitializer++));
             String callback = createCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();
             Log.getLogWriter().info("populateRegion: calling create for key " + key + ", object " + TestHelper.toString(vh) + ", region is " + aRegion.getFullPath());
             aRegion.create(key, vh, callback);
             Log.getLogWriter().info("populateRegion: done creating key " + key);
          }
        }
     }
  }

  /** Debug method to see what keys are on each VM at start of test
   *
   */
  public static void HydraTask_dumpLocalKeys() {
     testInstance.dumpLocalKeys();
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

    f = new ExecuteOp();
    FunctionService.registerFunction(f);

    Log.getLogWriter().info("Registered functions: " + FunctionService.getRegisteredFunctions());
  }

//----------------------------------------------------------------------------
// Tx operations
//----------------------------------------------------------------------------
  /*
   * Initialize information specific to transactional thread
   * (txVmPid is written to BB)
   */
  private void initializeTxInfo() {
    // This is the transactional thread -- let's put the VM PID into
    // the BB for use with local validation (since only local validators
    // will see this committed state with scope = LOCAL.
    Integer txVmPid = new Integer(RemoteTestModule.getMyPid());
    TxBB.getBB().getSharedMap().put(TxBB.TX_VM_PID, txVmPid);
    Log.getLogWriter().info("Putting txVmPid into BB at " + TxBB.TX_VM_PID + " with value = " + txVmPid);

    // write the clientName of the txVm to the BB (for use in OpList.java) by ViewTests. 
    String myClientName = RemoteTestModule.getMyClientName();
    TxBB.getBB().getSharedMap().put(TxBB.TX_VM_CLIENTNAME, myClientName);
    Log.getLogWriter().info("Putting clientVmName of txVm into BB at " + TxBB.TX_VM_CLIENTNAME + " with value " + myClientName);

    // Some view tests (mixed with non-tx threads) don't use the
    // TransactionListener or Writer.  Write default value to the BB for
    // those tests.  (The TxWriter will overwrite this if/when invoked.
    TxBB.getBB().getSharedMap().put(TxBB.TxWriterAction, TxBB.TXACTION_NONE);

    this.initializeTxInfo = false;
  }

  /*
   * Execute a transaction ... every other invocation either does a beginTx()
   * or finishTx().  Details shown below.
   * 
   * BEGIN:
   *  - clear out last known NON_TX_OPLIST (so we don't try to validate
   *    against it)
   *  - clear out any CacheListener_op lists from previous transaction
   *  - doOperations and post to BB
   *  - validate state (this thread should see tx state)
   *  - post info to BB for validator threads to use (TX_IN_PROGRESS, etc).
   *
   * FINISH:
   *  - validate state (this thread should see tx state)
   *  - commit or rollback (determined by TxPrms.commitPercentage)
   *  - validate state (based on commit vs. rollback)
   *  - update BB info (tx complete, completion action, etc).
   *
   */
  public static void HydraTask_executeTx() {
    // check for any listener exceptions thrown by previous method execution
    TestHelper.checkForEventError(TxBB.getBB());

    // Either setup or commit/rollback a transaction
    testInstance.executeTx();
  }

  protected void executeTx() {
    // on first invocation, save txVmPid in BB & obtain scope
    if (this.initializeTxInfo) {
      initializeTxInfo();
    }

    // take appropriate action (start or complete the tx)
    if (this.txInProgress) {
      TxHelper.internalResume(this.txContext);
      finishTx();
    } else {
      beginTx();
      this.txContext = TxHelper.internalSuspend();
    }

    // toggle between begin & commit/rollback
    this.txInProgress = !this.txInProgress;
  }

  /* 
   * txThread: start a transaction, perform operations and validation
   * tx view 
   */
  protected void beginTx() {
    // TX_NUMBER tracks current transaction (from begin through commit)
    long txNum = TxBB.getBB().getSharedCounters().incrementAndRead(TxBB.TX_NUMBER);
    Log.getLogWriter().info("In beginTx() for TX_NUMBER = " + txNum);

    // Let the listeners know that we're now ready to process events
    // wait a bit so any creates from createDestroyedRegionsFromBB(true)
    // have a chance to come through
    MasterController.sleepForMs(10000);
    TxBB.getBB().getSharedCounters().increment(TxBB.PROCESS_EVENTS);

    // clear out last known NON_TX_OPLIST since we're starting fresh
    // on this TX_NUMBER (removes KEY from SharedMap).
    Log.getLogWriter().fine("Clearing BB NON_TX_OPLIST");
    TxBB.getBB().getSharedMap().remove(TxBB.NON_TX_OPLIST);

    // clear out cacheListeners records (create, destroy, invalidate, put)
    Log.getLogWriter().info("Removing CacheListener operation lists from BB");
    TxBB.getBB().getSharedMap().remove(TxBB.LocalListenerOpListPrefix + (txNum-1));
    TxBB.getBB().getSharedMap().remove(TxBB.RemoteListenerOpListPrefix + (txNum-1));

    // validate cacheListener & txListener event counters from 
    // the previous transaction; match the expected counts established
    // from our collapsedOps list to the events processed
    // Note that this uses TxBB.COMPLETION_ACTION (TXACTION_COMMIT)
    // to determine if we expected events to be distributed.
    checkAndClearEventCounters();

    // random entry operations to be done here
    TxHelper.begin();

    // do random region/entry operations as part of our transaction
    // Save operation list to BB for other threads to use in validation
    // Proxy clients should not perform 'get' operations.  Since there
    // is no local storage, it always invokes the loader and oldValue is
    // always reported as null (which doesn't allow view validation).
    boolean allowGetOperations = true;
    OpList opList = doOperations(allowGetOperations);

    // save the list of operations
    TxBB.putOpList(opList);

    // post the key to get to the opList (for other threads to use)
    TxBB.getBB().getSharedMap().put(TxBB.TX_OPLIST_KEY, TxBB.getOpListKey());

    // verify we (txThread) see new tx state
    // note that region apis (containsKey(), containsValueForKey() and
    // getValueInVM() will operate on transaction state in this thread
    Log.getLogWriter().info("validating tx state");
    validateTxOps(true);

    // update expected counts (for listener tests)
    setExpectedEventCounts();

    // announce tx in progress to other threads
    SharedCounters sc = TxBB.getBB().getSharedCounters();
    sc.zero(TxBB.TX_COMPLETED);
    sc.increment(TxBB.TX_READY_FOR_VALIDATION);
  }

  /*
   * txThread: commit or rollback current transaction 
   */
  protected void finishTx() {

    // either commit or rollback & verify accordingly
    int n = this.rng.nextInt(1, 100);
    int commitPercentage = TxPrms.getCommitPercentage();

    boolean failedCommit = false;
    boolean txAborted = false;
    boolean isCausedByTransactionWriterException = false;
    if (n <= commitPercentage) {
      try {
        TxHelper.commit();
      } catch (ConflictException e) {
        failedCommit = true;
        TxBB.inc(TxBB.NUM_EXPECTED_FAILED_COMMIT);
        Log.getLogWriter().info("finishTx() caught " + e.toString());

        // Was this caused by a TxWriter aborting the TX?
        Throwable causedBy = e.getCause();
        if (causedBy instanceof TransactionWriterException) {
           isCausedByTransactionWriterException = true;
        }
      }

      // if a true CommitConflict, we should not have invoked the TxWriter
      if (failedCommit && !isCausedByTransactionWriterException) {
        if (!TxBB.getBB().getSharedMap().get(TxBB.TxWriterAction).equals(TxBB.TXACTION_NONE)) {
          throw new TestException("Invoked TransactionWriter after conflict detected " + TestHelper.getStackTrace());

        }
      }

      // The TransactionWriter (TxWriter) will occassionally abort the tx
      // If so, expect ConflictException Caused by TransactionWriterException
      // and invocation of afterFailedCommit()
      if (TxBB.getBB().getSharedMap().get(TxBB.TxWriterAction).equals(TxBB.TXACTION_ABORT)) {
        txAborted = true;
      }
      if (txAborted) {
        if (!failedCommit || !isCausedByTransactionWriterException) {
          throw new TestException("TransactionWriter threw TransactionWriterException (to abort tx), but caller did not process ConflictException Caused by TransactionWriterException " + TestHelper.getStackTrace());
        }
      }

      if (failedCommit && isCausedByTransactionWriterException) {
        if (!txAborted) {
          throw new TestException("Unexpected TX ABORT " + TestHelper.getStackTrace());
        }
      }

      if (failedCommit) {
        if (txAborted) {
          TxBB.getBB().getSharedMap().put(TxBB.COMPLETION_ACTION, TxBB.TXACTION_ABORT);
        } else {
          TxBB.getBB().getSharedMap().put(TxBB.COMPLETION_ACTION, TxBB.TXACTION_ROLLBACK);
        }
        validateCommittedState(false);
      } else {
        TxBB.getBB().getSharedMap().put(TxBB.COMPLETION_ACTION, TxBB.TXACTION_COMMIT);
        validateCommittedState(true);
      }
    } else {
      TxBB.inc(TxBB.NUM_EXPECTED_ROLLBACK);
      TxHelper.rollback();
      TxBB.getBB().getSharedMap().put(TxBB.COMPLETION_ACTION, TxBB.TXACTION_ROLLBACK);
      // verify after rollback invoked
      validateCommittedState(false);
    }

    // update shared state info in BB
    SharedCounters sc = TxBB.getBB().getSharedCounters();
    sc.zero(TxBB.TX_READY_FOR_VALIDATION);
    sc.increment(TxBB.TX_COMPLETED);
  }

  /*
   * txThread: validates final view (after commit/rollback)
   */
  protected void validateCommittedState(boolean txCommitted) {

    // PROXY clients cannot validate their committed state (they
    // have no local data to access
    if (this.isProxy) {
       return;
    }

    if (txCommitted) {
      Log.getLogWriter().info("TX COMPLETED: validating tx committed");
      validateCombinedOpsAfterCommit();
    } else {
      // We had a conflict, validate nonTx + any verifiable rolled back txOps
      Log.getLogWriter().info("TX COMPLETED: validating tx rollback");
      validateCombinedOpsAfterRollback();
    }
  }

  /* 
   * Used after commit to combine nonTxOpList & txOpList (since txOps
   * may have more recent values than the nonTxOpList, for example if
   * the nonTx op is a get and the txOp is an update on the same entry
   */
  protected void validateCombinedOpsAfterCommit() {
    OpList nonTxOpList = (OpList)TxBB.getBB().getSharedMap().get(TxBB.NON_TX_OPLIST);
    if (nonTxOpList == null) {          // simply validate txOps committed
      validateTxOps(true, TxBB.TXACTION_COMMIT);  // committed=>visible
      return;
    }
    
    // update newValues in nonTxOperations for any updates + 
    // add txOps (that are not updates) so they can be verified
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);

    OpList fullList = new OpList();
    // update newValues in nonTxOperations for any updates in txOps
    for (int i=0; i < nonTxOpList.numOps(); i++) {
      tx.Operation nonTxOp = nonTxOpList.getOperation(i);
      for (int j=0; j < txOpList.numOps(); j++) {
        tx.Operation txOp = txOpList.getOperation(j);
        if (txOp.isEntryGet()) {       // don't update ops newValue to earlier 'get' value
          continue;
        }
        if (nonTxOp.affectedBy(txOp)) {
          // if we destroyed a nonTx operation target entry in the tx, we
          // can't verify the nonTx op, don't add it to the list!
          if (txOp.isEntryDestroy()) {
            nonTxOp = null;
            break;
          }
          nonTxOp.setNewValue(txOp.getNewValue());
        } 
      }
      if (nonTxOp != null) {
        fullList.add(nonTxOp);
      }
    }

    // add verifiable txOps to our list to verify
    for (int i=0; i < txOpList.numOps(); i++) {
      tx.Operation txOp = txOpList.getOperation(i);
      for (int j=0; j < nonTxOpList.numOps(); j++) {
        tx.Operation nonTxOp = nonTxOpList.getOperation(j);
        // We can validate a destroy which affected a nonTx operation target entry
        if (txOp.affectedBy(nonTxOp)) {
          if (!txOp.isEntryDestroy()) {
            txOp = null;
            break;
          }
        } 
      }
      if (txOp != null) {
        fullList.add(txOp);
      }
    }
    Log.getLogWriter().fine("COMBINED NON-TX & TX OPS = " + fullList);
    validateState(fullList, true);
  }

  /*
   * Used in case of rollback to combine nonTxOpList with verifiable
   * operations from Tx.  (Operations that were rolled back and whose
   * values we're overridden by the nonTxOps).  Don't include any txOps
   * whose keys are not unique with respect to nonTx ops).
   */
  protected void validateCombinedOpsAfterRollback() {
    OpList nonTxOpList = (OpList)TxBB.getBB().getSharedMap().get(TxBB.NON_TX_OPLIST);
    if (nonTxOpList == null) {          // simply validate txOps rolledback
      validateTxOps(false, TxBB.TXACTION_ROLLBACK);  // rolledBack=>not visible
      return;
    }

    // verify nonTxOps - happened at operation time, should be committed state
    validateNonTxOps();

    // get the txOps not overridden by nonTxOps & verify NOT in current state
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);

    OpList shortList = new OpList();
    for (int i=0; i < txOpList.numOps(); i++) {
      tx.Operation txOp = txOpList.getOperation(i);
      for (int j=0; j < nonTxOpList.numOps(); j++) {
        tx.Operation nonTxOp = nonTxOpList.getOperation(j);
        if (txOp.affectedBy(nonTxOp)) {
          txOp = null;
          break;
        }
      }
      if (txOp != null) {
        shortList.add(txOp);
      }
    }
    Log.getLogWriter().fine("TXOPS NOT AFFECTED BY NON-TX OPS = " + shortList);
    validateState(shortList, false);
  }

  /*
   * Used by validator threads to check the final state
   * after the Tx has been committed.
   * Uses BB counters and sharedMap to determine if commit vs. rollback
   *
   * This thread may also perform non-transactional operations 
   * as configured by TxPrms.executeNonTxOperations.  If so, it 
   * performs the operations, then writes to a well-known map entry
   * (NON_TX_OPLIST) in the BB.  All validation checks will verify
   * this state (in distributed configurations) as well as the tx
   * state (provided by an OpListKey in the BB).
   */
  public static void HydraTask_checkCommittedState() {
    Log.getLogWriter().info("HydraTask_checkCommittedState(TX_NUMBER = " + TxBB.getBB().getSharedCounters().read(TxBB.TX_NUMBER) + ")" );
    // Hold off and allow distributions to occur
    MasterController.sleepForMs(1000);
    testInstance.checkCommittedState();

    // If we're done with the TX, replace any lost regions
    if (TxBB.getBB().getSharedCounters().read(TxBB.TX_COMPLETED) == 1) {
       // turn off listener processing (we'll be getting create events
       // from the createDestroyedRegionsFromBB() that we aren't interested in)
       // This gets set again by beginTx()
       TxBB.getBB().getSharedCounters().zero(TxBB.PROCESS_EVENTS);
    }
  }

  protected void checkCommittedState() {

    // If executeTx(begin) has executed, check that our validator
    // only sees the committed state and not the tx state
    if (TxBB.getBB().getSharedCounters().read(TxBB.TX_READY_FOR_VALIDATION) == 1) {
      // executeNonTxOperations - task attribute for non-tx threads
      // they can either be simple validators OR one additional thread
      // could also generate non-tx operations.
      boolean executeNonTxOperations = TxPrms.getExecuteNonTxOperations();
      Log.getLogWriter().info("TxPrms.getExecuteNonTxOperations() = " + executeNonTxOperations);

      if (executeNonTxOperations) {
        Log.getLogWriter().info("executing non-tx operations");
        OpList opList = doOperations();
        TxBB.getBB().getSharedMap().put(TxBB.NON_TX_OPLIST, opList);
        Log.getLogWriter().info("non-tx operations written to BB " + opList.toString());
      } else {
        Log.getLogWriter().info("check view, TX in progress, we should only see committed state");
      }
      // If we have a nonTx list, we can't validate that we see original 
      // values for txOps (because we may be causing conflicts with our 
      // nonTxOps.  Validate txOps when we can; just nonTxOps otherwise.
      if (!validateNonTxOps()) {
        validateTxOps(false);
      }
      return;
    }

    if (TxBB.getBB().getSharedCounters().read(TxBB.TX_COMPLETED) == 1) {

      // get the completion type (commit/rollback)
      String txAction = (String)TxBB.getBB().getSharedMap().get(TxBB.COMPLETION_ACTION);
      if (txAction.equals(TxBB.TXACTION_COMMIT)) {
        Log.getLogWriter().info("validate tx committed");
        validateCombinedOpsAfterCommit();   
      } else {
        Log.getLogWriter().info("validate tx rollback");
        validateCombinedOpsAfterRollback();   
      }
      return;
    }
    Log.getLogWriter().info("checkCommittedState: no tx state to verify, returning");
  }

  /*
   *  Validates both non-tx opList (if one exists) and the current
   *  state vs. the tx oplist.  Note that the non-tx operations should
   *  be seen by everyone (with distributed scope), while the tx operations
   *  may or not be seen based on the situation.  Our txOps validation is 
   *  therefore controlled by the boolean, isTxVisible.
   *
   *  @param - isTxVisible is only used while validating the current
   *           state vs. the tx opList.  It denotes whether the tx state
   *           should be seen by the thread performing validation.
   */
  protected void validateAllStates(boolean isTxVisible) {
    validateNonTxOps();
    validateTxOps(isTxVisible);
  }

  /*
   *  Validates non-tx opList (if one exists) 
   *
   *  @returns boolean to indicate if nonTxOps existed & were verified
   */
  protected boolean validateNonTxOps() {

    boolean nonTxOpsVerified = false;

    // Check non-tx state (if exists) -- note that non-tx operations
    // should always be visible (as long as distributed scope)
    // validateState() handles local scope differences
    OpList nonTxOpList = (OpList)TxBB.getBB().getSharedMap().get(TxBB.NON_TX_OPLIST);
    if (nonTxOpList != null) {
      Log.getLogWriter().info("validateAllStates() - verifying state with nonTxOpList");
      validateState(nonTxOpList, true);
      nonTxOpsVerified = true;
    }
    return nonTxOpsVerified;
  }

  /*
   *  Validates txOpList (may or may not be seen based on situation)
   *  Our validation is therefore controlled by the isTxVisible boolean.
   *
   *  @param - isTxVisible used while validating the current
   *           state vs. the tx opList.  It denotes whether the tx state
   *           should be seen by the thread performing validation.
   *  @return - boolean returned indicates that TxOps were found & verified
   */
  protected boolean validateTxOps(boolean isTxVisible) {
     return validateTxOps(isTxVisible, TxBB.TXACTION_NONE);
  }

  protected boolean validateTxOps(boolean isTxVisible, String txAction) {
    // check tx state (if exists)
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);
    Log.getLogWriter().info("validateAllStates() - verifying state with txOpList");
    validateState(txOpList, isTxVisible, txAction);
    return true;
  }

  /*
   *  Validates values in the cache vs. operations recently 
   *  executed as part of a transaction.
   *
   *  @param OpList opList - the opList to verify (may be Tx or non-tx
   *         opList from BB.
   *  @param boolean isVisible - should this thread 'see' the same state
   *         indicated by the transaction data in the BB?
   *         For example, 
   *           if tx not yet committed or rolledback invoke with isVisible=false
   *           to verify cache values against opList oldValue
   *         if tx committed, invoke with isVisible=true to verify cache values
   *           match those in newVal of opList
   */
  protected void validateState(OpList opList, boolean isVisible) {
     validateState(opList, isVisible, TxBB.TXACTION_NONE); 
  }

  protected void validateState(OpList opList, boolean isVisible, String txAction) {

    // replace with collapsed opList (multiple entry operations on same
    // region/key or region operations which affect region in current operation
    opList = opList.collapse(txAction);

    for (int i=0; i < opList.numOps(); i++) {
      tx.Operation op = opList.getOperation(i);

      // if local operation and we are not in the txVm, should not see mods
      if (op.isLocalOperation() && !inTxVm()) {
        Log.getLogWriter().fine("validateState() - isVisible forced to false");
        isVisible = false;
      }

      // whether or not this change is visible depends on tx state 
      // not yet committed, committed or rolledback AND the 
      if (op.isEntryOperation()) {
        EntryValidator expectedValues = EntryValidator.getExpected(op, isVisible);
        EntryValidator actualValues = EntryValidator.getActual(op);
        expectedValues.compare(actualValues);
      } else { // unexpected operation
       throw new TestException("Unknown operation " + op);
      }
    }
  }

/** 
 *  Check CacheWriter Event Counters
 */
protected void checkWriterCounters() {
   SharedCounters counters = MixedRegionBB.getBB().getSharedCounters();
   // "WRITER" counters reflect Writer operations (the local tx vm will see all)
   // for replicated regions, tx thread will invoke writer
   // for partition regions, writer invoked on primary dataStore
   long writerCreate = counters.read(MixedRegionBB.WRITER_CREATE);
   long writerCreateIsLoad = counters.read(MixedRegionBB.WRITER_CREATE_ISLOAD);
   long writerUpdate = counters.read(MixedRegionBB.WRITER_UPDATE);
   long writerUpdateIsLoad = counters.read(MixedRegionBB.WRITER_UPDATE_ISLOAD);
   long writerDestroy = counters.read(MixedRegionBB.WRITER_DESTROY);
   long writerInval = counters.read(MixedRegionBB.WRITER_INVALIDATE);

   // local ops not supported in this test
   long writerLocalDestroy = 0;
   long writerLocalInval = 0;

   ArrayList al = new ArrayList();
     // beforeCreate counters
        al.add(new ExpCounterValue("numBeforeCreateEvents_isDist", (writerCreate+writerCreateIsLoad)));
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNotDist", 0)); 
        al.add(new ExpCounterValue("numBeforeCreateEvents_isExp", 0)); 
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNotExp", (writerCreate+writerCreateIsLoad)));
        if (useLocalKeySet) {
          al.add(new ExpCounterValue("numBeforeCreateEvents_isRemote", 0));
          al.add(new ExpCounterValue("numBeforeCreateEvents_isNotRemote", (writerCreate+writerCreateIsLoad)));
        } else {
          al.add(new ExpCounterValue("numBeforeCreateEvents_isRemote", "numBeforeCreateEvents_isNotRemote", (writerCreate+writerCreateIsLoad)));
        }
        al.add(new ExpCounterValue("numBeforeCreateEvents_isLoad", writerCreateIsLoad));
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNotLoad", writerCreate));
        al.add(new ExpCounterValue("numBeforeCreateEvents_isLocalLoad", writerCreateIsLoad));
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNotLocalLoad", writerCreate));
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNetLoad", 0)); 
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNotNetLoad", (writerCreate+writerCreateIsLoad)));
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNetSearch", 0)); 
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNotNetSearch", (writerCreate+writerCreateIsLoad)));

      // beforeDestroy counters
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isDist", writerDestroy));
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotDist", 0)); 
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isExp", 0)); 
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotExp", writerDestroy));
        if (useLocalKeySet) {
          al.add(new ExpCounterValue("numBeforeDestroyEvents_isRemote", 0));
          al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotRemote", writerDestroy));
        } else {
          al.add(new ExpCounterValue("numBeforeDestroyEvents_isRemote", "numBeforeDestroyEvents_isNotRemote", writerDestroy));
        }
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isLoad", 0)); 
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotLoad", writerDestroy));
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isLocalLoad", 0)); 
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotLocalLoad", writerDestroy));
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNetLoad", 0)); 
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotNetLoad", writerDestroy));
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNetSearch", 0)); 
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotNetSearch", writerDestroy));

     // beforeUpdate counters
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isDist", (writerUpdate+writerUpdateIsLoad)));
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotDist", 0)); 
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isExp", 0)); 
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotExp", (writerUpdate+writerUpdateIsLoad)));
        if (useLocalKeySet) {
          al.add(new ExpCounterValue("numBeforeUpdateEvents_isRemote", 0));
          al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotRemote", (writerUpdate+writerUpdateIsLoad)));
        } else {
          al.add(new ExpCounterValue("numBeforeUpdateEvents_isRemote", "numBeforeUpdateEvents_isNotRemote", (writerUpdate+writerUpdateIsLoad)));
        }
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isLoad", writerUpdateIsLoad));
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotLoad", writerUpdate));
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isLocalLoad", writerUpdateIsLoad));
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotLocalLoad", writerUpdate));
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNetLoad", 0)); 
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotNetLoad", (writerUpdate+writerUpdateIsLoad)));
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNetSearch", 0)); 
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotNetSearch", (writerUpdate+writerUpdateIsLoad)));

     // beforeRegionDestroy counters
        al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isDist", 0));
        al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isNotDist", 0)); 
        al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isExp", 0)); 
        al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isNotExp", 0));
        al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isRemote", 0));
        al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isNotRemote", 0));

     WriterCountersBB.getBB().checkEventCounters(al);
}

/** 
 *  Check TxWriter Event Counters
 */
protected void checkTxWriterCounters() {
   SharedCounters counters = MixedRegionBB.getBB().getSharedCounters();
   // "WRITER" counters reflect Writer operations (the local tx vm will see all)
   // though, they will still be collapsed/conflated (updates to same entry will be collapsed into a single operation which requires us to use the REMOTE (Listener) counters vs. the Local (Writer) counters. 
   // for replicated regions, tx thread will invoke writer
   // for partition regions, writer invoked on primary dataStore
   // "LISTENER" counters reflect the events based on the collapsed opList
   long prWriterCreate = counters.read(MixedRegionBB.PR_LISTENER_CREATE);
   long prWriterCreateIsLoad = counters.read(MixedRegionBB.PR_LISTENER_CREATE_ISLOAD);
   long prWriterUpdate = counters.read(MixedRegionBB.PR_LISTENER_UPDATE);
   long prWriterUpdateIsLoad = counters.read(MixedRegionBB.PR_LISTENER_UPDATE_ISLOAD);
   long prWriterDestroy = counters.read(MixedRegionBB.PR_LISTENER_DESTROY);

   long rrWriterCreate = counters.read(MixedRegionBB.RR_LISTENER_CREATE);
   long rrWriterCreateIsLoad = counters.read(MixedRegionBB.RR_LISTENER_CREATE_ISLOAD);
   long rrWriterUpdate = counters.read(MixedRegionBB.RR_LISTENER_UPDATE);
   long rrWriterUpdateIsLoad = counters.read(MixedRegionBB.RR_LISTENER_UPDATE_ISLOAD);
   long rrWriterDestroy = counters.read(MixedRegionBB.RR_LISTENER_DESTROY);

   // add up PR + RR counters 
   long writerCreate = prWriterCreate + rrWriterCreate;
   long writerCreateIsLoad = prWriterCreateIsLoad + rrWriterCreateIsLoad;
   long writerUpdate = prWriterUpdate + rrWriterUpdate;
   long writerUpdateIsLoad = prWriterUpdateIsLoad + rrWriterUpdateIsLoad;
   long writerDestroy = prWriterDestroy + rrWriterDestroy;
   if (this.isProxy) {
      // allow for conflated entry create/destroy which is seen simply as a destroy   // in Tx VM (on commit only) when tx vm has DataPolicy.EMPTY
      long numConflatedCreateDestroy = counters.read(MixedRegionBB.CONFLATED_CREATE_DESTROY);

     writerDestroy += numConflatedCreateDestroy;
   }

   // local ops not supported in this test
   long writerLocalDestroy = 0;
   long writerLocalInval = 0;

   String endResult = (String)TxBB.getBB().getSharedMap().get(TxBB.COMPLETION_ACTION);
   // No result to check, try again later
   if (endResult == null) {
     return;
   }

   // If the tx fails or is rolled back, the remote listeners (cache & tx)
   // won't get any events, so take this into consideration via numTxCallbacks.
   // The TransactionWriter won't be invoked on Rollback, so we should expect
   // no events.
   if (!endResult.equals(TxBB.TXACTION_COMMIT)) {
     // If rollback, no invocation/events
     if (endResult.equals(TxBB.TXACTION_ROLLBACK)) {
       writerCreate = 0;
       writerCreateIsLoad = 0;
       writerUpdate = 0;
       writerUpdateIsLoad = 0;
       writerDestroy = 0;
     } // on rollback
   }   // if not committed

   ArrayList al = new ArrayList();
     // beforeCreate counters
        al.add(new ExpCounterValue("numCreateTxEvents_isDist", (writerCreate+writerCreateIsLoad)));
        al.add(new ExpCounterValue("numCreateTxEvents_isNotDist", 0)); 
        al.add(new ExpCounterValue("numCreateTxEvents_isExp", 0)); 
        al.add(new ExpCounterValue("numCreateTxEvents_isNotExp", (writerCreate+writerCreateIsLoad)));
        if (useLocalKeySet) {
          al.add(new ExpCounterValue("numCreateTxEvents_isRemote", 0));
          al.add(new ExpCounterValue("numCreateTxEvents_isNotRemote", (writerCreate+writerCreateIsLoad)));
        } else {
          // todo@lhughes -- writerCreateIsLoad should be isNotRemote for 
          // remote entries.  See BUG 41275 and change this when fixed.
          al.add(new ExpCounterValue("numCreateTxEvents_isRemote", (writerCreate+writerCreateIsLoad)));
          al.add(new ExpCounterValue("numCreateTxEvents_isNotRemote", 0));
        }
        al.add(new ExpCounterValue("numCreateTxEvents_isLoad", writerCreateIsLoad));
        al.add(new ExpCounterValue("numCreateTxEvents_isNotLoad", writerCreate));
        al.add(new ExpCounterValue("numCreateTxEvents_isLocalLoad", writerCreateIsLoad));
        al.add(new ExpCounterValue("numCreateTxEvents_isNotLocalLoad", writerCreate));
        al.add(new ExpCounterValue("numCreateTxEvents_isNetLoad", 0)); 
        al.add(new ExpCounterValue("numCreateTxEvents_isNotNetLoad", (writerCreate+writerCreateIsLoad)));
        al.add(new ExpCounterValue("numCreateTxEvents_isNetSearch", 0)); 
        al.add(new ExpCounterValue("numCreateTxEvents_isNotNetSearch", (writerCreate+writerCreateIsLoad)));

      // beforeDestroy counters
        al.add(new ExpCounterValue("numDestroyTxEvents_isDist", writerDestroy));
        al.add(new ExpCounterValue("numDestroyTxEvents_isNotDist", 0)); 
        al.add(new ExpCounterValue("numDestroyTxEvents_isExp", 0)); 
        al.add(new ExpCounterValue("numDestroyTxEvents_isNotExp", writerDestroy));
        if (useLocalKeySet) {
          al.add(new ExpCounterValue("numDestroyTxEvents_isRemote", 0));
          al.add(new ExpCounterValue("numDestroyTxEvents_isNotRemote", writerDestroy));
        } else {
          al.add(new ExpCounterValue("numDestroyTxEvents_isRemote", writerDestroy));
          al.add(new ExpCounterValue("numDestroyTxEvents_isNotRemote", 0));
        }
        al.add(new ExpCounterValue("numDestroyTxEvents_isLoad", 0)); 
        al.add(new ExpCounterValue("numDestroyTxEvents_isNotLoad", writerDestroy));
        al.add(new ExpCounterValue("numDestroyTxEvents_isLocalLoad", 0)); 
        al.add(new ExpCounterValue("numDestroyTxEvents_isNotLocalLoad", writerDestroy));
        al.add(new ExpCounterValue("numDestroyTxEvents_isNetLoad", 0)); 
        al.add(new ExpCounterValue("numDestroyTxEvents_isNotNetLoad", writerDestroy));
        al.add(new ExpCounterValue("numDestroyTxEvents_isNetSearch", 0)); 
        al.add(new ExpCounterValue("numDestroyTxEvents_isNotNetSearch", writerDestroy));

     // beforeUpdate counters
        al.add(new ExpCounterValue("numUpdateTxEvents_isDist", (writerUpdate+writerUpdateIsLoad)));
        al.add(new ExpCounterValue("numUpdateTxEvents_isNotDist", 0)); 
        al.add(new ExpCounterValue("numUpdateTxEvents_isExp", 0)); 
        al.add(new ExpCounterValue("numUpdateTxEvents_isNotExp", (writerUpdate+writerUpdateIsLoad)));
        if (useLocalKeySet) {
          al.add(new ExpCounterValue("numUpdateTxEvents_isRemote", 0));
          al.add(new ExpCounterValue("numUpdateTxEvents_isNotRemote", (writerUpdate+writerUpdateIsLoad)));
        } else {
          al.add(new ExpCounterValue("numUpdateTxEvents_isRemote", (writerUpdate+writerUpdateIsLoad)));
          al.add(new ExpCounterValue("numUpdateTxEvents_isNotRemote", 0));
        }
        al.add(new ExpCounterValue("numUpdateTxEvents_isLoad", writerUpdateIsLoad));
        al.add(new ExpCounterValue("numUpdateTxEvents_isNotLoad", writerUpdate));
        al.add(new ExpCounterValue("numUpdateTxEvents_isLocalLoad", writerUpdateIsLoad));
        al.add(new ExpCounterValue("numUpdateTxEvents_isNotLocalLoad", writerUpdate));
        al.add(new ExpCounterValue("numUpdateTxEvents_isNetLoad", 0)); 
        al.add(new ExpCounterValue("numUpdateTxEvents_isNotNetLoad", (writerUpdate+writerUpdateIsLoad)));
        al.add(new ExpCounterValue("numUpdateTxEvents_isNetSearch", 0)); 
        al.add(new ExpCounterValue("numUpdateTxEvents_isNotNetSearch", (writerUpdate+writerUpdateIsLoad)));

   TxWriterCountersBB.getBB().checkEventCounters(al);
}

/** 
 *  Check CacheListener Event Counters
 */
protected void checkEventCounters() {
   SharedCounters counters = MixedRegionBB.getBB().getSharedCounters();
   int numVmsWithList = TestHelper.getNumVMs();

   // "LISTENER" counters reflect the events based on the collapsed opList
   // which now (with prTx) applies to CacheListeners (invoked at commit time)
   // as well as TxWriters and TxListeners
   long prListenerCreate = counters.read(MixedRegionBB.PR_LISTENER_CREATE);
   long prListenerCreateIsLoad = counters.read(MixedRegionBB.PR_LISTENER_CREATE_ISLOAD);
   long prListenerUpdate = counters.read(MixedRegionBB.PR_LISTENER_UPDATE);
   long prListenerUpdateIsLoad = counters.read(MixedRegionBB.PR_LISTENER_UPDATE_ISLOAD);
   long prListenerDestroy = counters.read(MixedRegionBB.PR_LISTENER_DESTROY);
   long prListenerInval = counters.read(MixedRegionBB.PR_LISTENER_INVALIDATE);

   long rrListenerCreate = counters.read(MixedRegionBB.RR_LISTENER_CREATE);
   long rrListenerCreateIsLoad = counters.read(MixedRegionBB.RR_LISTENER_CREATE_ISLOAD);
   long rrListenerUpdate = counters.read(MixedRegionBB.RR_LISTENER_UPDATE);
   long rrListenerUpdateIsLoad = counters.read(MixedRegionBB.RR_LISTENER_UPDATE_ISLOAD);
   long rrListenerDestroy = counters.read(MixedRegionBB.RR_LISTENER_DESTROY);
   long rrListenerInval = counters.read(MixedRegionBB.RR_LISTENER_INVALIDATE);

   // allow for conflated entry create/destroy which is seen simply as a destroy
   // in Tx VM (on commit only) when tx vm has DataPolicy.EMPTY
   long numConflatedCreateDestroy = 0;
   if (this.isProxy) {
     numConflatedCreateDestroy = counters.read(MixedRegionBB.CONFLATED_CREATE_DESTROY);
   }
   long numClose = 0;

   // handle situation with rollback or commitConflict
   // remote listeners won't get any events in this case!
   String endResult = (String)TxBB.getBB().getSharedMap().get(TxBB.COMPLETION_ACTION);
   // We don't actually have a result yet
   if (endResult == null) 
     return;

   // On Rollback, we shouldn't get any CacheListener events (set all expected counters to 0)
   if (!endResult.equals(TxBB.TXACTION_COMMIT)) {
     prListenerCreate = 0;
     prListenerCreateIsLoad = 0;
     prListenerUpdate = 0;
     prListenerUpdateIsLoad = 0;
     prListenerDestroy = 0;
     prListenerInval = 0;
 
     rrListenerCreate = 0;
     rrListenerCreateIsLoad = 0;
     rrListenerUpdate = 0;
     rrListenerUpdateIsLoad = 0;
     rrListenerDestroy = 0;
     rrListenerInval = 0;
     numConflatedCreateDestroy = 0;
   }

   Log.getLogWriter().info("For PartitionedRegions, expect callbacks from " + numPRsWithList + " dataStores.");
   Log.getLogWriter().info("For ReplicatedRegions, expect callbacks from " + numVmsWithList + " VMs.");

   ArrayList al = new ArrayList();
        // afterCreate counters
           al.add(new ExpCounterValue("numAfterCreateEvents_isDist", (((prListenerCreate + prListenerCreateIsLoad) * numPRsWithList) + ((rrListenerCreate + rrListenerCreateIsLoad) * numVmsWithList))));
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotExp", (((prListenerCreate + prListenerCreateIsLoad) * numPRsWithList) + ((rrListenerCreate + rrListenerCreateIsLoad) * numVmsWithList)))); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isRemote", "numAfterCreateEvents_isNotRemote", (((prListenerCreate+prListenerCreateIsLoad) * numPRsWithList) + ((rrListenerCreate+rrListenerCreateIsLoad) * numVmsWithList))));
           al.add(new ExpCounterValue("numAfterCreateEvents_isLoad", ((prListenerCreateIsLoad * numPRsWithList) + (rrListenerCreateIsLoad * numVmsWithList)))); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotLoad", ((prListenerCreate * numPRsWithList) + (rrListenerCreate * numVmsWithList))));
           al.add(new ExpCounterValue("numAfterCreateEvents_isLocalLoad", ((prListenerCreateIsLoad*numPRsWithList) + (rrListenerCreateIsLoad*numVmsWithList))));
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotLocalLoad", ((prListenerCreate * numPRsWithList) + (rrListenerCreate * numVmsWithList))));
           al.add(new ExpCounterValue("numAfterCreateEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotNetLoad", (((prListenerCreate+prListenerCreateIsLoad) * numPRsWithList) + ((rrListenerCreate+rrListenerCreateIsLoad) * numVmsWithList)))); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotNetSearch", (((prListenerCreate+prListenerCreateIsLoad) * numPRsWithList) + ((rrListenerCreate+rrListenerCreateIsLoad) * numVmsWithList)))); 

        // afterDestroy counters
           al.add(new ExpCounterValue("numAfterDestroyEvents_isDist", ((prListenerDestroy * numPRsWithList) + (rrListenerDestroy * numVmsWithList)+numConflatedCreateDestroy)));
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotExp", ((prListenerDestroy * numPRsWithList) + ((rrListenerDestroy * numVmsWithList)+numConflatedCreateDestroy)))); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isRemote", "numAfterDestroyEvents_isNotRemote", ((prListenerDestroy * numPRsWithList) + ((rrListenerDestroy * numVmsWithList)+numConflatedCreateDestroy))));
           al.add(new ExpCounterValue("numAfterDestroyEvents_isLoad", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotLoad", ((prListenerDestroy * numPRsWithList) + ((rrListenerDestroy * numVmsWithList)+numConflatedCreateDestroy)))); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isLocalLoad", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotLocalLoad", ((prListenerDestroy * numPRsWithList) + ((rrListenerDestroy * numVmsWithList)+numConflatedCreateDestroy)))); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotNetLoad", ((prListenerDestroy * numPRsWithList) + ((rrListenerDestroy * numVmsWithList)+numConflatedCreateDestroy)))); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotNetSearch", ((prListenerDestroy * numPRsWithList) +  ((rrListenerDestroy * numVmsWithList)+numConflatedCreateDestroy)))); 

        // afterInvalidate counters
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isDist", ((prListenerInval * numPRsWithList) +  (rrListenerInval * numVmsWithList))));
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotExp", ((prListenerInval * numPRsWithList) + (rrListenerInval * numVmsWithList)))); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isRemote", "numAfterInvalidateEvents_isNotRemote", ((prListenerInval * numPRsWithList) + (rrListenerInval * numVmsWithList))));
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isLoad", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotLoad", ((prListenerInval * numPRsWithList) + (rrListenerInval * numVmsWithList)))); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isLocalLoad", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotLocalLoad", ((prListenerInval * numPRsWithList) + (rrListenerInval * numVmsWithList)))); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotNetLoad", ((prListenerInval * numPRsWithList) + (rrListenerInval * numVmsWithList)))); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotNetSearch", ((prListenerInval * numPRsWithList) + (rrListenerInval * numVmsWithList)))); 

        // afterUpdate counters
           al.add(new ExpCounterValue("numAfterUpdateEvents_isDist", (((prListenerUpdate + prListenerUpdateIsLoad) * numPRsWithList) + ((rrListenerUpdate + rrListenerUpdateIsLoad) * numVmsWithList))));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotExp", (((prListenerUpdate + prListenerUpdateIsLoad) * numPRsWithList) + ((rrListenerUpdate + rrListenerUpdateIsLoad) * numVmsWithList)))); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isRemote", "numAfterUpdateEvents_isNotRemote", (((prListenerUpdate+prListenerUpdateIsLoad) * numPRsWithList) + ((rrListenerUpdate+rrListenerUpdateIsLoad) * numVmsWithList))));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isLoad", ((prListenerUpdateIsLoad * numPRsWithList) + (rrListenerUpdateIsLoad * numVmsWithList)))); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLoad", ((prListenerUpdate * numPRsWithList) + (rrListenerUpdate * numVmsWithList))));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isLocalLoad", ((prListenerUpdateIsLoad*numPRsWithList) + (rrListenerUpdateIsLoad*numVmsWithList))));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLocalLoad", ((prListenerUpdate * numPRsWithList) + (rrListenerUpdate * numVmsWithList))));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotNetLoad", (((prListenerUpdate + prListenerUpdateIsLoad) * numPRsWithList) + ((rrListenerUpdate + rrListenerUpdateIsLoad) * numVmsWithList)))); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotNetSearch", (((prListenerUpdate + prListenerUpdateIsLoad) * numPRsWithList) + ((rrListenerUpdate + rrListenerUpdateIsLoad) * numVmsWithList)))); 

        // afterRegionDestroy counters
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isDist", 0));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotExp", 0));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isRemote", 0));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotRemote", 0));

        // afterRegionInvalidate counters
           al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isDist", 0));
           al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isNotDist", 0));
           al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isNotExp", 0));
           al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isRemote", 0));
           al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isNotRemote", 0));

        // after(Region) close counters
           al.add(new ExpCounterValue("numClose", numClose, false));

    EventCountersBB.getBB().checkEventCounters(al);
}

/** 
 *  Check TxListener Event Counters
 */
protected void checkTxEventCounters() {
   SharedCounters counters = MixedRegionBB.getBB().getSharedCounters();
   int numVmsWithList = TestHelper.getNumVMs();
   int numPrsWithList = numPRsWithList;  // we may need to overwrite this global value after rollback
                                         // so work with a local copy in this method

   // "LISTENER" counters reflect the events based on the collapsed opList
   long prListenerCreate = counters.read(MixedRegionBB.PR_LISTENER_CREATE);
   long prListenerCreateIsLoad = counters.read(MixedRegionBB.PR_LISTENER_CREATE_ISLOAD);
   long prListenerUpdate = counters.read(MixedRegionBB.PR_LISTENER_UPDATE);
   long prListenerUpdateIsLoad = counters.read(MixedRegionBB.PR_LISTENER_UPDATE_ISLOAD);
   long prListenerDestroy = counters.read(MixedRegionBB.PR_LISTENER_DESTROY);
   long prListenerInval = counters.read(MixedRegionBB.PR_LISTENER_INVALIDATE);

   long rrListenerCreate = counters.read(MixedRegionBB.RR_LISTENER_CREATE);
   long rrListenerCreateIsLoad = counters.read(MixedRegionBB.RR_LISTENER_CREATE_ISLOAD);
   long rrListenerUpdate = counters.read(MixedRegionBB.RR_LISTENER_UPDATE);
   long rrListenerUpdateIsLoad = counters.read(MixedRegionBB.RR_LISTENER_UPDATE_ISLOAD);
   long rrListenerDestroy = counters.read(MixedRegionBB.RR_LISTENER_DESTROY);
   long rrListenerInval = counters.read(MixedRegionBB.RR_LISTENER_INVALIDATE);

   // allow for conflated entry create/destroy which is seen simply as a destroy
   // in Tx VM (on commit only) when tx vm has DataPolicy.EMPTY
   long numConflatedCreateDestroy = 0;
   if (this.isProxy) {
     numConflatedCreateDestroy = counters.read(MixedRegionBB.CONFLATED_CREATE_DESTROY);
   }

   // handle situation with rollback or commitConflict
   // remote listeners won't get any events in this case!
   String endResult = (String)TxBB.getBB().getSharedMap().get(TxBB.COMPLETION_ACTION);
   // We don't actually have a result yet
   if (endResult == null) 
     return;

   // On Rollback, we shouldn't get any CacheListener events (set all expected counters to 0)
   if (!endResult.equals(TxBB.TXACTION_COMMIT)) {
     // If the tx fails or is rolled back, the remote tx listeners won't get any events
     numPrsWithList = 1;
     numVmsWithList = 1;
     numConflatedCreateDestroy = 0;
   }

   Log.getLogWriter().info("For PartitionedRegions, expect callbacks from " + numPrsWithList + " dataStores.");
   Log.getLogWriter().info("For ReplicatedRegions, expect callbacks from " + numVmsWithList + " VMs.");

   ArrayList al = new ArrayList();
        // afterCreate counters
           al.add(new ExpCounterValue("numCreateTxEvents_isDist", (((prListenerCreate + prListenerCreateIsLoad) * numPrsWithList) + ((rrListenerCreate + rrListenerCreateIsLoad) * numVmsWithList))));
           al.add(new ExpCounterValue("numCreateTxEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numCreateTxEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNotExp", (((prListenerCreate + prListenerCreateIsLoad) * numPrsWithList) + ((rrListenerCreate + rrListenerCreateIsLoad) * numVmsWithList)))); 
           al.add(new ExpCounterValue("numCreateTxEvents_isRemote", "numCreateTxEvents_isNotRemote", (((prListenerCreate+prListenerCreateIsLoad) * numPrsWithList) + ((rrListenerCreate+rrListenerCreateIsLoad) * numVmsWithList))));
           al.add(new ExpCounterValue("numCreateTxEvents_isLoad", ((prListenerCreateIsLoad * numPrsWithList) + (rrListenerCreateIsLoad * numVmsWithList)))); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNotLoad", ((prListenerCreate * numPrsWithList) + (rrListenerCreate * numVmsWithList))));
           al.add(new ExpCounterValue("numCreateTxEvents_isLocalLoad", ((prListenerCreateIsLoad*numPrsWithList) + (rrListenerCreateIsLoad*numVmsWithList))));
           al.add(new ExpCounterValue("numCreateTxEvents_isNotLocalLoad", ((prListenerCreate * numPrsWithList) + (rrListenerCreate * numVmsWithList))));
           al.add(new ExpCounterValue("numCreateTxEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNotNetLoad", (((prListenerCreate+prListenerCreateIsLoad) * numPrsWithList) + ((rrListenerCreate+rrListenerCreateIsLoad) * numVmsWithList)))); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNotNetSearch", (((prListenerCreate+prListenerCreateIsLoad) * numPrsWithList) + ((rrListenerCreate+rrListenerCreateIsLoad) * numVmsWithList)))); 

        // afterDestroy counters
           al.add(new ExpCounterValue("numDestroyTxEvents_isDist", ((prListenerDestroy * numPrsWithList) + + (rrListenerDestroy * numVmsWithList)+numConflatedCreateDestroy)));
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotExp", ((prListenerDestroy * numPrsWithList) + ((rrListenerDestroy * numVmsWithList)+numConflatedCreateDestroy)))); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isRemote", "numDestroyTxEvents_isNotRemote", ((prListenerDestroy * numPrsWithList) + ((rrListenerDestroy * numVmsWithList)+numConflatedCreateDestroy))));
           al.add(new ExpCounterValue("numDestroyTxEvents_isLoad", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotLoad", ((prListenerDestroy * numPrsWithList) + ((rrListenerDestroy * numVmsWithList)+numConflatedCreateDestroy)))); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isLocalLoad", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotLocalLoad", ((prListenerDestroy * numPrsWithList) + ((rrListenerDestroy * numVmsWithList)+numConflatedCreateDestroy)))); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotNetLoad", ((prListenerDestroy * numPrsWithList) + ((rrListenerDestroy * numVmsWithList)+numConflatedCreateDestroy)))); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotNetSearch", ((prListenerDestroy * numPrsWithList) +  ((rrListenerDestroy * numVmsWithList)+numConflatedCreateDestroy)))); 

        // afterInvalidate counters
           al.add(new ExpCounterValue("numInvalidateTxEvents_isDist", ((prListenerInval * numPrsWithList) +  (rrListenerInval * numVmsWithList))));
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotExp", ((prListenerInval * numPrsWithList) + (rrListenerInval * numVmsWithList)))); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isRemote", "numInvalidateTxEvents_isNotRemote", ((prListenerInval * numPrsWithList) + (rrListenerInval * numVmsWithList))));
           al.add(new ExpCounterValue("numInvalidateTxEvents_isLoad", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotLoad", ((prListenerInval * numPrsWithList) + (rrListenerInval * numVmsWithList)))); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isLocalLoad", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotLocalLoad", ((prListenerInval * numPrsWithList) + (rrListenerInval * numVmsWithList)))); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotNetLoad", ((prListenerInval * numPrsWithList) + (rrListenerInval * numVmsWithList)))); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotNetSearch", ((prListenerInval * numPrsWithList) + (rrListenerInval * numVmsWithList)))); 

        // afterUpdate counters
           al.add(new ExpCounterValue("numUpdateTxEvents_isDist", (((prListenerUpdate + prListenerUpdateIsLoad) * numPrsWithList) + ((rrListenerUpdate + rrListenerUpdateIsLoad) * numVmsWithList))));
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotExp", (((prListenerUpdate + prListenerUpdateIsLoad) * numPrsWithList) + ((rrListenerUpdate + rrListenerUpdateIsLoad) * numVmsWithList)))); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isRemote", "numUpdateTxEvents_isNotRemote", (((prListenerUpdate+prListenerUpdateIsLoad) * numPrsWithList) + ((rrListenerUpdate+rrListenerUpdateIsLoad) * numVmsWithList))));
           al.add(new ExpCounterValue("numUpdateTxEvents_isLoad", ((prListenerUpdateIsLoad * numPrsWithList) + (rrListenerUpdateIsLoad * numVmsWithList)))); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotLoad", ((prListenerUpdate * numPrsWithList) + (rrListenerUpdate * numVmsWithList))));
           al.add(new ExpCounterValue("numUpdateTxEvents_isLocalLoad", ((prListenerUpdateIsLoad*numPrsWithList) + (rrListenerUpdateIsLoad*numVmsWithList))));
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotLocalLoad", ((prListenerUpdate * numPrsWithList) + (rrListenerUpdate * numVmsWithList))));
           al.add(new ExpCounterValue("numUpdateTxEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotNetLoad", (((prListenerUpdate + prListenerUpdateIsLoad) * numPrsWithList) + ((rrListenerUpdate + rrListenerUpdateIsLoad) * numVmsWithList)))); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotNetSearch", (((prListenerUpdate + prListenerUpdateIsLoad) * numPrsWithList) + ((rrListenerUpdate + rrListenerUpdateIsLoad) * numVmsWithList)))); 

    TxEventCountersBB.getBB().checkEventCounters(al);
}


  // Code to support event counters
  // There are three different types of counters (expected, cacheListener, txListener)
  // Each type has counters for create, destroy, invalidate and update
  // At executeTx(begin), all counters are cleared and the expected counts built
  // based on the collapsed opList (*numVMs)
  // cacheListener - updated as cacheListener events processed
  // txListeners - updated on receipt of TxEvent
  // validation is done as the last part of executeTx(finish).

  static final int WRITER_EVENTS = 0;
  static final int LISTENER_EVENTS = 1;
  private void setExpectedEventCounts() {

    if (!this.checkEventCounters) {
      return;
    }
 
    // clear existing counters
    MixedRegionBB.getBB().zeroEventCounters();

    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);
 
    // update expected counters specific to the cache writer
    incrementEventCounters(txOpList, WRITER_EVENTS);

    // update expected counters for the remote VMs listeners
    incrementEventCounters(txOpList.getEntriesWithUniqueKeys(txOpList.getEntryOps()), LISTENER_EVENTS);
  }


  /** updatedInThisTx returns a boolean which indicates whether
   *  or not an update on this same Object (region, key match) occurred
   *  after the get/load in this same transaction.
   */
  private boolean updatedInThisTx(tx.Operation op) {
    boolean rc = false;

    // Get the original & complete opList
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);

    String regionName = op.getRegionName();
    String key = (String)op.getKey();

    // find our 'get' (op) and see if updated after the load
    int i = 0;
    for (i=0; i < txOpList.numOps(); i++) {
      tx.Operation tmpOp = txOpList.getOperation(i);
      if (op.equals(tmpOp)) break;  // only interested in what happened BEFORE op
    }

    // we are now indexing the passed in get/load op, move past this and continue
    for (i++; i < txOpList.numOps(); i++) {
      tx.Operation tmpOp = txOpList.getOperation(i);
      if (tmpOp.isEntryUpdate() && tmpOp.usesSameRegionAndKey(op)) {
        Log.getLogWriter().info("updatedInThisTx returning TRUE + updateOperation = " + tmpOp.toString() + " occurred after get/loadOp = " + op.toString());
        rc = true;
        break;
      }
    }
    return rc;
  }

  /** invalidatedInThisTx returns a boolean which indicates whether
   *  or not a invalidate on this same Object (region, key match) occurred
   * in this same transaction (prior to op)
   */
  private boolean invalidatedInThisTx(tx.Operation op) {
    boolean rc = false;

    // Get the original & complete opList
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);

    String regionName = op.getRegionName();
    String key = (String)op.getKey();
    for (int i=0; i < txOpList.numOps(); i++) {
      tx.Operation tmpOp = txOpList.getOperation(i);
      if (op.equals(tmpOp)) break;  // only interested in what happened BEFORE op
      if (tmpOp.isEntryInvalidate() && tmpOp.usesSameRegionAndKey(op)) {
        Log.getLogWriter().info("invalidatedInThisTx returning TRUE + invalidateOperation = " + tmpOp.toString() + " occurred before op = " + op.toString());
        rc = true;
        break;
      }
    }
    return rc;
  }

  /** createdInThisTx returns a boolean which indicates whether
   *  or not an entry was created and destroyed within the same tx (in that order).
   *  The destroyOperation is passed in, true is returned if that same entry
   *  was created earlier in the same tx.
   *  (Note that remote VMs will not get a TxEvent for entries which are
   *  created and destroyed within the same tx).
   */
  private boolean createdInThisTx(tx.Operation op) {
    boolean rc = false;

    // Get the original & complete opList
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);

    String regionName = op.getRegionName();
    String key = (String)op.getKey();
    for (int i=0; i < txOpList.numOps(); i++) {
      tx.Operation tmpOp = txOpList.getOperation(i);
      if (op.equals(tmpOp)) break;   // only interested in creates prior to DESTROY
      if (tmpOp.isEntryCreate() && tmpOp.usesSameRegionAndKey(op)) {
        Log.getLogWriter().info("createdInThisTx returning TRUE, createOperation = " + tmpOp.toString() + " occurred prior to destroyOp = " + op.toString());
        rc = true;
        break;
      }
    }
    return rc;
  }

  /** destroyedBeforeLoadInThisTx returns a boolean which indicates whether
   *  or not an entry was destroyed prior to a get/load within the same tx.
   *  The get operation is passed in, true is returned if that same entry
   *  was destroyed earlier in the same tx.
   *  Note that this appears as a create to the local (tx) VM and as an 
   *  update to remote VMs (since they just get the update their previously
   *  (non-destroyed) entry.
   */
  private boolean destroyedBeforeLoadInThisTx(tx.Operation op) {
    boolean rc = false;

    // Get the original & complete opList
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);

    String regionName = op.getRegionName();
    String key = (String)op.getKey();
    for (int i=0; i < txOpList.numOps(); i++) {
      tx.Operation tmpOp = txOpList.getOperation(i);
      if (op.equals(tmpOp)) break;   // only interested in destroy prior to LOAD
      if (tmpOp.isEntryDestroy() && tmpOp.usesSameRegionAndKey(op)) {
        Log.getLogWriter().info("destroyedBeforeLoad returning TRUE + destroyOperation = " + tmpOp.toString() + " occurred prior to getOperation = " + op.toString());
        rc = true;
        break;
      }
    }
    return rc;
  }

  /** destroyedInThisTx returns a boolean which indicates whether
   *  or not an entry was created and destroyed within the same tx (in that order).
   *  The createOperation is passed in, true is returned if that same entry
   *  was subsequently destroyed in the same tx.
   *  (Note that remote VMs will not get a TxEvent for entries which are
   *  created and destroyed within the same tx).
   */
  private boolean destroyedInThisTx(tx.Operation op) {
    boolean rc = false;

    // Get the original & complete opList
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);

    String regionName = op.getRegionName();
    String key = (String)op.getKey();
    // start with the create operation, then look for subsequent destroy
    int i = 0;
    Log.getLogWriter().fine("Searching for original (create) operation in OpList, op = " + op.toString());
    while (i < txOpList.numOps()) {
      tx.Operation tmpOp = txOpList.getOperation(i);
      if (op.equals(tmpOp)) break;   // only interested in creates followed by destroy
      Log.getLogWriter().fine("operation " + tmpOp.toString() + " does not match createOp = " + op.toString());
      i++;
    }
    Log.getLogWriter().fine("Found original operation in opList " + op.toString());

    // txOpList[i] = the createOperation (passed in)
    // We are now looking at the createOperation, get past that to next op
    Log.getLogWriter().fine("Searching for an entry-destroy on same region/key as createOp  " + op.toString());
    for (i++; i< txOpList.numOps(); i++) {
      tx.Operation tmpOp = txOpList.getOperation(i);
      if (tmpOp.isEntryDestroy() && tmpOp.usesSameRegionAndKey(op)) {
        Log.getLogWriter().info("destroyedInThisTx returning TRUE + destroyOperation = " + tmpOp.toString() + " occurred prior to createOp = " + op.toString());
        rc = true;
        break;
      }
      Log.getLogWriter().fine("Operation " + tmpOp.toString() + " does not match createOp = " + op.toString());
    }
    return rc;
  }


  public void incrementEventCounters(OpList txOpList, int counterType) {

    for (int i=0; i < txOpList.numOps(); i++) {
      boolean isPartitioned = false;
      tx.Operation op = txOpList.getOperation(i);
      String opName = op.getOpName();
      // Listener event can be invoked in multiple VMs ... track PR counts
      // seperately from DR (DistributedRegions).
      if (counterType == LISTENER_EVENTS) {
        Region aRegion = RegionHelper.getRegion(op.getRegionName());
        if (PartitionRegionHelper.isPartitionedRegion(aRegion)) {
          isPartitioned = true; 
        } 
      }
     
      // We need to differentiate between isLoad & notLoad events!
      boolean isLoad = false;

      // handle gets specifically: may end up as no-op (exists), create (new) or update (if previously invalidated)
      if (op.isEntryGet()) {
        if (op.isPreviouslyInvalid() || invalidatedInThisTx(op)) {   // invalid, so key exists
          opName = tx.Operation.ENTRY_UPDATE;
          isLoad = true;
        } else if (destroyedBeforeLoadInThisTx(op)) {
           // If we did a destroy prior to get (load) in this same Tx, it would
           // appear as a create in the local VM, but as updates in the remote VMs
           opName = (counterType==WRITER_EVENTS) ? tx.Operation.ENTRY_CREATE : tx.Operation.ENTRY_UPDATE;
        } else if (op.getOldValue() == null) {         // destroyed => load & new value
          opName = tx.Operation.ENTRY_CREATE;
          isLoad = true;

          // get/load + update = distributed create (isNotLoad)
          if (counterType == LISTENER_EVENTS) {
            if (updatedInThisTx(op)) {
              isLoad = false;
            }
          }
        } 
      }

      // update appropriate counter
      // Note that we aren't interested in region ops or in gets that
      // didn't result in a loader operation (create or update).
      SharedCounters sc = MixedRegionBB.getBB().getSharedCounters();
      if (opName.equalsIgnoreCase(tx.Operation.ENTRY_CREATE)) {
         if (counterType == LISTENER_EVENTS) {   
             if (isPartitioned) {
               sc.increment((isLoad) ? MixedRegionBB.PR_LISTENER_CREATE_ISLOAD : MixedRegionBB.PR_LISTENER_CREATE);
             } else {
               sc.increment((isLoad) ? MixedRegionBB.RR_LISTENER_CREATE_ISLOAD : MixedRegionBB.RR_LISTENER_CREATE);
             }
         } else {                               // WRITER events
           if (destroyedInThisTx(op)) {
             sc.increment(MixedRegionBB.CONFLATED_CREATE_DESTROY);
           } 
           sc.increment((isLoad) ? MixedRegionBB.WRITER_CREATE_ISLOAD : MixedRegionBB.WRITER_CREATE);
         }

         // transactions will not invoke netLoad
      } else if (opName.equalsIgnoreCase(tx.Operation.ENTRY_UPDATE)) {
         //transactions will not invoke netLoad
         if (counterType == LISTENER_EVENTS) {
           if (isPartitioned) {
             sc.increment((isLoad) ? MixedRegionBB.PR_LISTENER_UPDATE_ISLOAD : MixedRegionBB.PR_LISTENER_UPDATE);
           } else {
             sc.increment((isLoad) ? MixedRegionBB.RR_LISTENER_UPDATE_ISLOAD : MixedRegionBB.RR_LISTENER_UPDATE);
           }
         } else {     // WRITER_EVENTS
           sc.increment((isLoad) ? MixedRegionBB.WRITER_UPDATE_ISLOAD : MixedRegionBB.WRITER_UPDATE);
         }
      } else if (opName.equalsIgnoreCase(tx.Operation.ENTRY_DESTROY)) {
         // if an entry is created and destroyed within the same tx, remote vms
         // will NOT see either in the TxEvent
         if (counterType == LISTENER_EVENTS) {
            if (!createdInThisTx(op)) {
               sc.increment((isPartitioned) ? MixedRegionBB.PR_LISTENER_DESTROY : MixedRegionBB.RR_LISTENER_DESTROY);
            }
         } else sc.increment(MixedRegionBB.WRITER_DESTROY);
      } else if (opName.equalsIgnoreCase(tx.Operation.ENTRY_INVAL)) {
         if (!op.isDoubleInvalidate()) {
           if (counterType == LISTENER_EVENTS) {
             sc.increment((isPartitioned) ? MixedRegionBB.PR_LISTENER_INVALIDATE : MixedRegionBB.RR_LISTENER_INVALIDATE);
           } else {  // WRITER_EVENT
             sc.increment(MixedRegionBB.WRITER_INVALIDATE);
           }
         }
      } 
    }
    MixedRegionBB.getBB().printSharedCounters();
  }

  private void checkAndClearEventCounters() {

    if (!this.checkEventCounters) {
      return;
    }

    // CacheWriter events
    checkWriterCounters();

    // Transaction Writer events
    checkTxWriterCounters();

    // CacheListener events
    checkEventCounters();

    // Transaction Listener events
    checkTxEventCounters();
     
    // clear event counters (local & remote) 
    EventCountersBB.getBB().zeroAllCounters();
    WriterCountersBB.getBB().zeroAllCounters();
    TxEventCountersBB.getBB().zeroAllCounters();
    TxWriterCountersBB.getBB().zeroAllCounters();

    // clear TxWriterAction
    TxBB.getBB().getSharedMap().put(TxBB.TxWriterAction, TxBB.TXACTION_NONE);
    
  }

// ======================================================================== 
// methods for task execution and round robin counters
// ======================================================================== 

/** Log the number of tasks that have been executed in this test.
 *  Useful for debugging/analyzing serial execution tests.
 */
public static void logExecutionNumber() {
   long exeNum = TxBB.getBB().getSharedCounters().incrementAndRead(TxBB.ExecutionNumber);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
}

/** Log the "round" number for this test. This is called for serial execution
 *  tests that use round-robin scheduling. Each task can call this method and
 *  it will remember the first thread in the round, then increment and log a
 *  counter each time a new round begins.
 *
 *  @returns True if the current thread is the first one in a round, false
 *           otherwise.
 */
public static boolean logRoundRobinNumber() {
   Blackboard BB = TxBB.getBB();
   String rrStartThread = (String)(BB.getSharedMap().get(TxBB.RoundRobinStartThread));
   String currentThreadName = Thread.currentThread().getName();
   if (rrStartThread == null) { // first task execution; save who is the first in the round
      rrStartThread = currentThreadName;
      BB.getSharedMap().put(TxBB.RoundRobinStartThread, rrStartThread);
   }
   if (currentThreadName.equals(rrStartThread)) { // starting a new round
      long rrNum = TxBB.getBB().getSharedCounters().incrementAndRead(TxBB.RoundRobinNumber);
      Log.getLogWriter().info("Beginning round " + rrNum);
      return true;
   }
   return false;
}

/** Return the current roundRobin number.
 */
public static long getRoundRobinNumber() {
   return TxBB.getBB().getSharedCounters().read(TxBB.RoundRobinNumber);
}

  /** Return a currently existing random region. 
   *
   *  @param allowRootRegion true if this call can return the root region, false otherwise.
   *
   *  @return A random region, or null if none available. 
   */
  public Region getRandomRegion(boolean allowRootRegion) {
     Region aRegion = null;
     String regionName = TestConfig.tab().stringAt(TxPrms.excludeRegionName, null);
     if (regionName != null) {
        aRegion = CacheUtil.getCache().getRegion(regionName);
     }
     return getRandomRegion(allowRootRegion, aRegion, NO_REPLICATION_RESTRICTION);
  }

  /** Return a currently existing random region.  
   *
   *  @param allowRootRegion true if this call can return the root region, false otherwise.
   *  @param restriction (forces selection of Partitioned vs. Replicated Regions)
   *
   *  @return A random region, or null of none available.
   */
  public Region getRandomRegion(boolean allowRootRegion, int restriction) {
     Region aRegion = null;
     String regionName = TestConfig.tab().stringAt(TxPrms.excludeRegionName, null);
     if (regionName != null) {
        aRegion = CacheUtil.getCache().getRegion(regionName);
     }
     return getRandomRegion(allowRootRegion, aRegion, restriction);
  }

  /** Return a currently existing random region. 
   *
   *  @param allowRootRegion true if this call can return the root region, false otherwise.
   *  @param excludeRegion Return a region other than this region, or null if none to exclude.
   *  @param restriction Restriction on what region can be returned, for example
   *            NO_REPLICATION_RESTRICTION, NO_REPLICATION
   *
   *  @return A random region, or null of none available. 
   */
  public Region getRandomRegion(boolean allowRootRegion, Region excludeRegion, int restriction) {
     // Get the set of all regions available
     Object[] rootRegionArr = CacheUtil.getCache().rootRegions().toArray();
     if (rootRegionArr.length == 0)
        return null;
     ArrayList regionList = new ArrayList();
     if (allowRootRegion) {
        for (int i = 0; i < rootRegionArr.length; i++) 
           regionList.add(rootRegionArr[i]);
     };
     for (int i = 0; i < rootRegionArr.length; i++) {
        Region rootRegion = (Region)(rootRegionArr[i]);
        Object[] regionArr = getSubregions(rootRegion, true).toArray();
        for (int j = 0; j < regionArr.length; j++) 
           regionList.add(regionArr[j]);
     }

     // choose a random region
     if (regionList.size() == 0)
        return null;
     int randInt = TestConfig.tab().getRandGen().nextInt(0, regionList.size() - 1);
     Region aRegion = (Region)regionList.get(randInt);
     if ((restriction != NO_REPLICATION_RESTRICTION) || (excludeRegion != null)) { // we have a restriction
        int startIndex = randInt;
        boolean done = true;
        do {
           done = true;
           try {
              if (restriction == USE_PARTITIONED_REGION) {
                 done = PartitionRegionHelper.isPartitionedRegion(aRegion);
              }
              if (restriction == USE_REPLICATED_REGION)
                 done = isHierReplicated(aRegion);
              if ((excludeRegion != null) && (aRegion.getFullPath().equals(excludeRegion.getFullPath())))
                 done = false;
           } catch (RegionDestroyedException e) {
              done = false;
           }
           if (done)
              break;
           randInt++; // go to the next region
           if (randInt == regionList.size()) // wrap if necessary
              randInt = 0; 
           if (randInt == startIndex) { // went all the way through regionList
              return null;
           }
           aRegion = (Region)(regionList.get(randInt));
        } while (!done);
     } 
     return aRegion;
  }

// ======================================================================== 
// methods to do random operations
// ======================================================================== 

/** Do random operations on random regions using the hydra parameter 
 *  TxPrms.operations as the list of available operations and 
 *  TxPrms.numOps as the number of operations to do.
 */
public static OpList doOperations() {
   boolean allowGetOperations = true;
   return doOperations(allowGetOperations);
}

/** Do random operations on random regions using the hydra parameter 
 *  TxPrms.operations as the list of available operations and 
 *  TxPrms.numOps as the number of operations to do.
 *  
 *  @param allowGetOperations - to prevent proxy clients from attempting
 *                              gets (since they have no storage this always
 *                              invokes the loader and oldValue is always
 *                              reported as null).
 */
public static OpList doOperations(boolean allowGetOperations) {
   Vector operations = TestConfig.tab().vecAt(TxPrms.operations);
   int numOps = TestConfig.tab().intAt(TxPrms.numOps);
   return testInstance.doOperations(operations, numOps, allowGetOperations); 
}

/** Do random operations on random regions.
 *  
 *  @param operations - a Vector of operations to choose from.
 *  @param numOperationsToDo - the number of operations to execute.
 *
 *  @returns An instance of OpList, which is a list of operations that
 *           were executed.
 */
public OpList doOperations(Vector operations, int numOperationsToDo) {
   boolean allowGetOperations = true;
   return doOperations(operations, numOperationsToDo, allowGetOperations);
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
   Log.getLogWriter().info("Executing " + numOperationsToDo + " random operations...");
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

      // override get operations with create (for proxy view clients)
      if (operation.startsWith("entry-get") && !allowGetOperations) {
         operation = tx.Operation.ENTRY_CREATE;
      }

      Log.getLogWriter().info("Operation is " + operation);

      // force region to be Partitioned for first op of tx (to Bind Tx to single dataStore)
      int restriction = USE_PARTITIONED_REGION;
      if (numOpsCompleted > 0) {
        if (this.rng.nextBoolean()) {
          restriction = NO_REPLICATION_RESTRICTION;
        }
      }
      // todo@lhughes -- take this out once remote mixed tx ops supported
      if (PrTxPrms.alwaysUsePartitionedRegions()) {
        restriction = USE_PARTITIONED_REGION;
      }

      Region aRegion = getRandomRegion(true, restriction);
  
      if (operation.equalsIgnoreCase(tx.Operation.ENTRY_CREATE)) {
         op = createEntry(aRegion);
      } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_UPDATE)) {
         op = updateEntry(aRegion, getRandomKey(aRegion));
      } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_DESTROY)) {
         op = destroyEntry(aRegion, getRandomKey(aRegion));
      } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_INVAL)) {
         op = invalEntry(aRegion, getRandomKey(aRegion));
      } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_GET_NEW_KEY)) {
         op = getEntryWithNewKey(aRegion);
      } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_GET_EXIST_KEY)) {
         op = getWithExistingKey(aRegion);
      } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_GET_PREV_KEY)) {
         op = getEntryWithPreviousKey(aRegion);
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

// ======================================================================== 
// methods to do operations on region entries
// ======================================================================== 

/** Does a creates for a key.
 *  
 *  @param aRegion The region to create the new key in.
 *  
 *  @return An instance of Operation describing the create operation.
 */
public tx.Operation createEntry(Region aRegion) {
   Object key = getNewKey(aRegion);
   Object oldValue = null;
   BaseValueHolder vh = new ValueHolder(key, randomValues, new Integer(modValInitializer++));
   String callback = createCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();

   Log.getLogWriter().info("createEntry: putting key " + key + ", object " + vh.toString() + " in region " + aRegion.getFullPath());
   if (useFunctionExecution && PartitionRegionHelper.isPartitionedRegion(aRegion)) {
     executeOp(aRegion, tx.Operation.ENTRY_CREATE, key, vh, callback);
   } else  {
     try {
        aRegion.create(key, vh, callback);
     } catch (RegionDestroyedException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     } catch (EntryExistsException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
     }
   }
   Log.getLogWriter().info("createEntry: done putting key " + key + ", object " + vh.toString() + " in region " + aRegion.getFullPath());

   return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_CREATE, oldValue, vh.modVal);
}

/** Updates an existing entry in aRegion. The value in the key
 *  is increment by 1.
 *  
 *  @param aRegion The region to modify a key in.
 *  @param key The key to modify.
 *
 *  @return An instance of Operation describing the update operation.
 */
public tx.Operation updateEntry(Region aRegion, Object key) {
   if (key == null) {
      Log.getLogWriter().info("Could not update a key in " + aRegion.getFullPath() + " because no keys are available");
      return null;
   }

   // get the old value
   BaseValueHolder vh = null;

   Log.getLogWriter().info("updateEntry: Getting value to prepare for update for key " + key + " in region " + aRegion.getFullPath());

   Object oldValue = getValueInVM(aRegion, key);
   if (oldValue instanceof BaseValueHolder) {
      vh = (BaseValueHolder)oldValue;
      oldValue = ((BaseValueHolder)oldValue).modVal;
   } else {
      vh = new ValueHolder(key, randomValues, new Integer(modValInitializer++));
   }

   Log.getLogWriter().info("updateEntry: Value to update is " + vh + " for key " + key + " in region " + aRegion.getFullPath());

   // we MUST use CopyHelper here (vs. copyOnRead) since we are using
   // getValueInVM() vs. a public 'get' api
   vh = (BaseValueHolder)CopyHelper.copy(vh);
   vh.modVal = new Integer(vh.modVal.intValue() + 1);
   String callback = updateCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();

   Log.getLogWriter().info("updateEntry: Putting new value " + vh + " for key " + key + " in region " + aRegion.getFullPath());
   if (useFunctionExecution && PartitionRegionHelper.isPartitionedRegion(aRegion)) {
     executeOp(aRegion, tx.Operation.ENTRY_UPDATE, key, vh, callback);
   } else {
     try {
        aRegion.put(key, vh, callback);
     } catch (RegionDestroyedException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
     }
   }

   Log.getLogWriter().info("updateEntry: Done putting new value " + vh + " for key " + key + " in region " + aRegion.getFullPath());
   return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_UPDATE, oldValue, vh.modVal);
}

/** Destroys an entry in aRegion. 
 *  
 *  @param aRegion The region to destroy the entry in.
 *  @param key The key to destroy.
 *
 *  @return An instance of Operation describing the destroy operation.
 */
public tx.Operation destroyEntry(Region aRegion, Object key) {
   if (key == null) {
      Log.getLogWriter().info("Could not destroy an entry in " + aRegion.getFullPath() + " because no keys are available");
      return null;
   }
   try {
      Object oldValue = getValueInVM(aRegion, key);
      if (oldValue instanceof BaseValueHolder) {
         oldValue = ((BaseValueHolder)oldValue).modVal;
      }
      String callback = destroyCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();

      Log.getLogWriter().info("destroyEntry: destroying key " + key + " in region " + aRegion.getFullPath());
      if (useFunctionExecution && PartitionRegionHelper.isPartitionedRegion(aRegion)) {
        executeOp(aRegion, tx.Operation.ENTRY_DESTROY, key, null, callback);
      } else {
        aRegion.destroy(key, callback);
      }

      Log.getLogWriter().info("destroyEntry: done destroying key " + key + " in region " + aRegion.getFullPath());
      return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_DESTROY, oldValue, null);
   } catch (RegionDestroyedException e) { 
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (EntryNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }

}

/** Invalidates an entry in aRegion. 
 *  
 *  @param aRegion The region to invalidate the entry in.
 *  @param key The key to invalidate.
 *
 *  @return An instance of Operation describing the invalidate operation.
 */
public tx.Operation invalEntry(Region aRegion, Object key) {
   if (key == null) {
      Log.getLogWriter().info("Could not invalidate an entry in " + aRegion.getFullPath() + " because no keys are available");
      return null;
   }

   try {
      String callback = invalidateCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();

      Object oldValue = getValueInVM(aRegion, key);
      if (oldValue instanceof BaseValueHolder) {
         oldValue = ((BaseValueHolder)oldValue).modVal;
      }
      Log.getLogWriter().info("invalEntry: invalidating key " + key + " in region " + aRegion.getFullPath());
      if (useFunctionExecution && PartitionRegionHelper.isPartitionedRegion(aRegion)) {
        executeOp(aRegion, tx.Operation.ENTRY_INVAL, key, null, callback);
      } else {
        aRegion.invalidate(key, callback);
      }

      Log.getLogWriter().info("invalEntry: done invalidating key " + key + " in region " + aRegion.getFullPath());
      Object newValue = getValueInVM(aRegion, key);
      return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_INVAL, oldValue, newValue);
   } catch (RegionDestroyedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (EntryNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Gets a value in aRegion with a key existing in aRegion.
 *  
 *  @param aRegion The region to get from.
 *
 *  @return An instance of Operation describing the get operation.
 */
public tx.Operation getWithExistingKey(Region aRegion) {
   Object key = getRandomKey(aRegion);
   if (key == null) {
      Log.getLogWriter().info("Could not get with an existing key " + aRegion.getFullPath() + " because no keys are available");
      return null;
   }
   try {
      Object oldValue = getValueInVM(aRegion, key);
      Log.getLogWriter().info("getWithExistingKey: getting value for key " + key + " in region " + aRegion.getFullPath());
      if (oldValue instanceof BaseValueHolder) {
         oldValue = ((BaseValueHolder)oldValue).modVal;
      }

      BaseValueHolder vh = null;
      if (useFunctionExecution && PartitionRegionHelper.isPartitionedRegion(aRegion)) {
        vh = (BaseValueHolder)executeOp(aRegion, tx.Operation.ENTRY_GET_EXIST_KEY, key, null, null);
      } else {
        vh = (BaseValueHolder)(aRegion.get(key));
      }

      Log.getLogWriter().info("getWithExistingKey: got value for key " + key + ": " + vh + " in region " + aRegion.getFullPath());
      if (vh == null) {
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_GET_EXIST_KEY, oldValue, null);
      } else {
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_GET_EXIST_KEY, oldValue, vh.modVal);
     }
   } catch (RegionDestroyedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Gets a value in aRegion with a random key that was previously used in the test.
 *  
 *  @param aRegion The region to get from.
 *
 *  @return An instance of Operation describing the get operation.
 */
public tx.Operation getEntryWithPreviousKey(Region aRegion) {

   Object key;
   if (PartitionRegionHelper.isPartitionedRegion(aRegion)) {
     key = getRandomKey(aRegion);
   } else {
     long keysUsed = NameFactory.getPositiveNameCounter();
     key = NameFactory.getObjectNameForCounter(TestConfig.tab().getRandGen().nextInt(1, (int)keysUsed));
   }

   try {
      Object oldValue = getValueInVM(aRegion, key);
      if (oldValue instanceof BaseValueHolder) {
         oldValue = ((BaseValueHolder)oldValue).modVal;
      }
      Log.getLogWriter().info("getEntryWithPreviousKey: getting value for key " + key + " in region " + aRegion.getFullPath());

      BaseValueHolder vh = null;
      if (useFunctionExecution && PartitionRegionHelper.isPartitionedRegion(aRegion)) {
        vh = (BaseValueHolder)executeOp(aRegion, tx.Operation.ENTRY_GET_PREV_KEY, key, null, null);
      } else {
        vh = (BaseValueHolder)(aRegion.get(key));
      }

      Log.getLogWriter().info("getEntryWithPreviousKey: got value for key " + key + ": " + vh + " in region " + aRegion.getFullPath());
      if (vh == null) {
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_GET_PREV_KEY, oldValue, null);
      } else {
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_GET_PREV_KEY, oldValue, vh.modVal);
      }
   } catch (RegionDestroyedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Gets a new (never before seen) key
 *
 *  @return the next key from NameFactory
 */
public Object getNewKey(Region aRegion) {
   Object key;
   if (PartitionRegionHelper.isPartitionedRegion(aRegion)) {
     key = getNewKeyForPartitionedRegion(aRegion);
   } else {
     key = NameFactory.getNextPositiveObjectName();
   }
   return key;
}

/** Gets a new (never before seen) key.
 *  Invoked by createEntry and getEntryWithNewKey
 *
 *  @return the next key (with hashCode) for the target VM
 */
/*
public Object getNewKeyForPartitionedRegion(Region aRegion) { 
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
  public Object getNewKeyForPartitionedRegion(Region aRegion) { 

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

/** Gets a value in aRegion with a the next new (never-before_used) key.
 *  
 *  @param aRegion The region to get from.
 *
 *  @return An instance of Operation describing the get operation.
 */
public tx.Operation getEntryWithNewKey(Region aRegion) {
   Object key = getNewKey(aRegion);
   try {
      Object oldValue = getValueInVM(aRegion, key);
      Log.getLogWriter().info("getEntryWithNewKey: getting value for key " + key + " in region " + aRegion.getFullPath());
      BaseValueHolder vh = null;
      if (useFunctionExecution && PartitionRegionHelper.isPartitionedRegion(aRegion)) {
        vh = (BaseValueHolder)executeOp(aRegion, tx.Operation.ENTRY_GET_EXIST_KEY, key, null, null);
      } else {
        vh = (BaseValueHolder)(aRegion.get(key));
      }
      Log.getLogWriter().info("getEntryWithNewKey: got value for key " + key + ": " + vh + " in region " + aRegion.getFullPath());
      if (vh == null) {
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_GET_NEW_KEY, oldValue, null);
      } else {
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_GET_NEW_KEY, oldValue, vh.modVal);
      }
   } catch (RegionDestroyedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Puts a new key/value in the given region.
 *  
 *  @param aRegion The region to create the new key in.
 *  @param key The key to use for the put.
 *  @param value The value to put.
 *  @param opName The operation to use for the returned Operation instance.
 *  
 *  @return An instance of Operation describing the put operation.
 */
public tx.Operation putEntry(Region aRegion, Object key, BaseValueHolder value, String opName) {
   Object oldValue = getValueInVM(aRegion, key);
   if (oldValue instanceof BaseValueHolder) {
      oldValue = ((BaseValueHolder)oldValue).modVal;
   }
   try {
      String callback = updateCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();
      Log.getLogWriter().info("putEntry: putting key " + key + ", object " + value + " in region " + aRegion.getFullPath());
      aRegion.put(key, value, callback);
      Log.getLogWriter().info("putEntry: done putting key " + key + ", object " + value + " in region " + aRegion.getFullPath());
   } catch (RegionDestroyedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   return new tx.Operation(aRegion.getFullPath(), key, opName, oldValue, value.modVal);
}

// ======================================================================== 
// other methods 
// ======================================================================== 

/** Get a random key from the given region. If no keys are present in the
 *  region, return null.
 *
 *  @param aRegion - The region to get the key from.
 *
 *  @returns A key from aRegion.
 */
public Object getRandomKey(Region aRegion) {
   return getRandomKey(aRegion, null);
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
   if (aRegion == null) {
      return null;
   }
   Set aSet = null;
   try {
      if (PartitionRegionHelper.isPartitionedRegion(aRegion)) {
        // get thread specific keySet from BB
        String mapKey = PrTxBB.keySet + aRegion.getFullPath();
        KeySetResult keySetResult = (KeySetResult)PrTxBB.getBB().getSharedMap().get(mapKey);
        List aList = keySetResult.getKeySet();
        aSet = new HashSet(aList);
      } else {
        aSet = new HashSet(((LocalRegion)aRegion).testHookKeys());
      }
   } catch (RegionDestroyedException e) {
        throw new TestException(TestHelper.getStackTrace(e));
   }

   Object[] keyArr = aSet.toArray();
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
   return key;
}

public Object getNewValue(Object key) {
   return new ValueHolder(key, randomValues, new Integer(modValInitializer++));
}

static public boolean inTxThreadWithTxInProgress() {
  return (TxHelper.getTransactionId()==null) ? false : true;
}

/*
 * Helper function to determine if we're in the transacational VM (not
 * necessarily the txThread -- helps when we validate state for LOCAL scope
 */
static public boolean inTxVm() {
  Integer txVmPid = (Integer)TxBB.getBB().getSharedMap().get(TxBB.TX_VM_PID);
  if (txVmPid == null) { // tests not using tx (like AsyncMsgTests which also use operations/opList)
    return false;
  }
  int myVmPid = ProcessMgr.getProcessId();

  return (txVmPid.intValue()==myVmPid);
}

/** Return true if the region or any of its subregions is replicated,
 *  return false otherwise.
 *
 *  @param aRegion The region parent to test for replication.
 *
 */
public boolean isHierReplicated(Region aRegion) {
   boolean isReplicated = aRegion.getAttributes().getDataPolicy().withReplication();
   if (isReplicated)
      return true;
   Object[] regionArr = getSubregions(aRegion, true).toArray();
   for (int j = 0; j < regionArr.length; j++) {
      Region subR = (Region)(regionArr[j]);
      if (subR.getAttributes().getDataPolicy().withReplication())
         return true; 
   }
   return false;
}

/** Return a Set of subregions of the given region, while handling
 *  regionDestroyedExceptions.
 *
 *  @param aRegion - The region to get subregions of.
 *  @param recursive - If true, return all subregions, otherwise
 *                     only aRegion's subregions.
 *
 *  @returns A set of all subregions. If aRegion has been
 *           destroyed, return an empty set.
 */
public Set getSubregions(Region aRegion, boolean recursive) {
   try {
      Set regionSet = aRegion.subregions(recursive);
      return regionSet;
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e; // not expected in serial tests
      // This is OK for a concurrent test as long as the exception
      // is thrown for aRegion and not one of its children
      String regionName = aRegion.getFullPath();
      String errorRegionName = e.getRegionFullPath();
      if (!regionName.equals(errorRegionName)) // exception caused by a child
         throw e; 
      return new HashSet();
   }
}

/** Call getValueInVM on the given region and key using suspend and resume.
 *
 *  @param aRegion - The region to use for the call.
 *  @param key - The key to use for the call.
 *
 *  @returns The value in the VM of key.
 */
public Object getValueInVM(Region aRegion, Object key) {
   Object value = null;
   Region.Entry entry = aRegion.getEntry(key);
   if (entry != null) {
     value = entry.getValue();
     if (value == null) {    // keyExists with null value => INVALID
       value = Token.INVALID;
     }
   }
   return value;
}

// KeySet methods

  /** Initialize remote keySet.
   *
   *  Writes a KeySetResult (with DistributedMember and keySet) to PrTxBB.
   *  getRandomKey() relies upon this (to select colocated entries on
   *  a single remote VM).
   *  getNewKey() also relies on this (so it can determine the correct 
   *  hashCode required for a new entry on the remote VM).
   */
  protected void initializeKeySet(boolean useLocalKeySet) {

    // suspend the transaction (function execution is not allowed within tx)
    TXStateInterface txState = TxHelper.internalSuspend();
    DistributedMember targetDM = null;

    // For the local VM, we can set the targetDM now.
    // For remote VMs, we'll get the target DM from the first call to getKeySet
    if (useLocalKeySet) {
       targetDM = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
    }

    // get the keySet for each region (either locally or on a single remote vm)
    Set rootRegions = CacheUtil.getCache().rootRegions();
    for (Iterator it = rootRegions.iterator(); it.hasNext(); ) {
       Region aRegion = (Region)it.next();
       if (PartitionRegionHelper.isPartitionedRegion(aRegion)) {
         // Set a keySet (specific to a random member) for this round of execution
         // getRandomKey and getNewKeyForRegion depend on this being set in the BB
         KeySetResult keySetResult = getKeySet(aRegion, targetDM);
         String mapKey = PrTxBB.keySet + aRegion.getFullPath();
         PrTxBB.getBB().getSharedMap().put(mapKey, keySetResult);
         targetDM = keySetResult.getDistributedMember();
         List keyList = (List)keySetResult.getKeySet();
         Object[] keySet = keyList.toArray();
         Log.getLogWriter().info("KeySet for " + targetDM + "  = " + keyList);
      }
    }
    // resume transaction
    TxHelper.internalResume(txState);
  }

  protected KeySetResult getKeySet(Region aRegion, DistributedMember targetDM) {
    KeySetResult ksResult = null;

    DistributedMember localDM = DistributedSystemHelper.getDistributedSystem().getDistributedMember();

    Function f = FunctionService.getFunction("parReg.tx.GetKeySet");
    Execution e = FunctionService.onRegion(aRegion).withArgs(localDM.toString());

    Log.getLogWriter().info("executing " + f.getId() + " on region " + aRegion.getName());
    ResultCollector rc = e.execute(f);
    Log.getLogWriter().info("executed " + f.getId());

    List results = (List)rc.getResult(); 
    DistributedMember remoteDM = null;
    boolean keySetAvailable = false;
    for (Iterator rit = results.iterator(); rit.hasNext(); ) {
       ksResult = (KeySetResult)rit.next();
       remoteDM = ksResult.getDistributedMember();

       // targetDM is null on the first invocation for a remote VM 
       // it is set to the local DM if we want local keySets
       if (targetDM == null) {               // looking for a remote VM
          if (!localDM.equals(remoteDM)) {   // take first remote VM as target
             targetDM = remoteDM;
          }
       } 

       if (targetDM != null) {
          if (remoteDM.equals(targetDM)) {
             // we've got a keySet for the local dataStore
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

  /** Update (per region) keySet 
   *  Region specific keySets are maintained through each invocation
   *  of doOperations().  They are initialized at the beginning, but keys
   *  need to be removed on entryDestroy and added on entryCreate.
   *  We don't have a test hook to look at the TxState (vs. committed state).
   *
   *  This does nothing if the region is replicated (vs. partitioned).
   */
   protected void updateKeySet(tx.Operation op) {
     String regionName = op.getRegionName();
     if (PartitionRegionHelper.isPartitionedRegion(RegionHelper.getRegion(regionName))) {
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
  }

  /** Execute the specified operation with (key, value) provided
   *  on the given region.
   */
  protected Object executeOp(Region aRegion, String opName, Object key, Object value, String callback) {

    Log.getLogWriter().info("Invoking ExecuteOp with " + opName + "(" + key + ", " + value + ")");
    Function f = FunctionService.getFunction("parReg.tx.ExecuteOp");
    DistributedSystem ds = CacheHelper.getCache().getDistributedSystem();
    DistributedMember dm = ds.getDistributedMember();

    // target appropriate VM
    Set filter = new HashSet();
    filter.add(key);

    // build args ArrayList (op: key, value) 
    ArrayList aList = new ArrayList();
    aList.add(dm.toString());
    aList.add(opName);
    aList.add(key);
    aList.add(value);
    aList.add(callback);
    Execution e = FunctionService.onRegion(aRegion).withArgs(aList).withFilter(filter);

    Log.getLogWriter().info("executing " + f.getId() + " with filter " + filter);
    ResultCollector rc = e.execute(f.getId());
    Log.getLogWriter().info("executed " + f.getId());

    List result = (List)(rc.getResult());
    BaseValueHolder vh = (BaseValueHolder)(result.get(0));
    return(vh);
  }
}
