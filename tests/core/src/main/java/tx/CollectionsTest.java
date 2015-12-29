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
                                                                               
package tx;
                                                                               
import java.util.*;
import java.lang.reflect.*;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.Token;

import hydra.*;
import util.*;
                                                                               
/**
 * A test to validate behavior of region collections
 * Region collections (keys, values, entries, subRegions)
 * and getEntry all have a common set of behaviors.
 *
 * === Old (pre 6.5 behavior) ===
 * When obtained outside of a transaction, Collections will
 * reflect committed state (note that the Collection is 
 * always backed up by the true region contents).  When the 
 * iterator is obtained is important however ... it reflects
 * the collection at the time the iterator was created.
 *
 * If a collection is obtained between begin() & commit/rollback, 
 * it reflects transactional state (though at least for now, the
 * contents of the Collection will not change dynamically while
 * the transacation state changes).  If the collection or iterator
 * is accessed outside the transaction, an IllegalStateException 
 * is thrown.
 * 
 * Entries obtained via Region.getEntry() work the same way -- if
 * obtained outside the transaction, it reflects committed state.
 * If obtained within the transaction, transactional state.  If 
 * subsequently accessed outside of the transaction, it will throw
 * an IllegalStateException
 *
 * == New (6.5 behavior) ==
 *
 * Collections obtained outside of a transaction cannot be used within it.
 * Collections obtained within a transaction may not be used outside it.
 * In both cases above, an IllegalStateException will be thrown.
 *
 * Note that region operations are not transactional and Region.subregions() is
 * not subject to the limitations above.
 *
 * To see IllegalStateExceptions being logged, enable 'fine' logging.
 *
 */
                                                                               
public class CollectionsTest {
//-----------------------------------------------------------------------
// Class variables
//-----------------------------------------------------------------------
static protected CollectionsTest collectionsTest;
                                                                               
//-----------------------------------------------------------------------
// Instance variables
//-----------------------------------------------------------------------
RandomValues randomValues = null;
int modValInitializer=0;          // track ValueHolder.modVal
long executionNumber = 0;        
                                                                               
  /*
   *  Initialize info for test (TBD)
   */
  public static void HydraTask_initialize() {
    Log.getLogWriter().info("In HydraTask_initialize");
    if (collectionsTest == null) {
      collectionsTest = new CollectionsTest();
      collectionsTest.initialize();
    }
  }

  public void initialize() {
    this.randomValues = new RandomValues();
  }

  public static void HydraTask_doTransactions() {
    Log.getLogWriter().info("In HydraTask_doTransactions");
    Vector operations = TestConfig.tab().vecAt(TxPrms.operations);

    // Each transaction will only contain 1 operation
    collectionsTest.doTransactions(operations);
  }

  private void doTransactions(Vector operations) {
    final long TIME_LIMIT_MS = 60000;  
    long taskStartTime = System.currentTimeMillis();

    while (System.currentTimeMillis() - taskStartTime < TIME_LIMIT_MS) {

      // TX_NUMBER tracks current transaction (from begin through commit)
      //long txNum = TxBB.getBB().getSharedCounters().incrementAndRead(TxBB.TX_NUMBER);
      Log.getLogWriter().info("doTransactions: EXECUTION NUMBER = " + ++executionNumber);

      // Build up a list of operations (without actually doing them)
      // We need to know the operation ahead of time so we can save
      // the region collection data specific to that region and entry.
      // Note: Each operation will be done in a single transaction
      Operation op = getOperation(operations);

      // save pertinent region info (keys, values, entries, selected entry,
      // subregions of the parent region for region ops)
      Region aRegion = CacheUtil.getCache().getRegion(op.getRegionName());
      CollectionInfo originalData = new CollectionInfo(aRegion, op);
      Log.getLogWriter().info("Original Collections  = \n" + originalData.toString());
  
      // re-create any destroyedRegions
      TxUtil.txUtilInstance.clearDestroyedRegions();

      // beginTx
      TxHelper.begin();
  
      // execute the given operation
      executeOperation(op);
  
      // get txRegionInfo (keys, values, entries)
      CollectionInfo txData = new CollectionInfo(aRegion, op);
      Log.getLogWriter().info("Transaction Collections = \n" + txData.toString());
  
      // we should not be able to access originalData (sets) within the tx
      // verify correct data in Sets for txData (obtained within tx)
      validateInTx(op, originalData, txData);

      // commit
      try {
        TxHelper.commit();
      } catch (ConflictException e) {
        throw new TestException("Unexpected ConflictException " + TestHelper.getStackTrace(e));
      }
  
      // outside of tx, get a new iterator and verify we see updates
      validateAfterCommit(op, originalData, txData);

      // re-create any destroyed regions
      TxUtil.txUtilInstance.createAllDestroyedRegions(true);
    }
  }

  private Operation getOperation(Vector operations) {

    Operation op = null;
    String operation = (String)(operations.get(TestConfig.tab().getRandGen().nextInt(0, operations.size()-1)));

    int attempts = 0;
    while (op == null) {
      if (operation.equalsIgnoreCase(Operation.ENTRY_CREATE)) {
         op = getCreateEntryOp(TxUtil.txUtilInstance.getRandomRegion(true));
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_UPDATE)) {
         Region aRegion = TxUtil.txUtilInstance.getRandomRegion(true);
         op = getUpdateEntryOp(aRegion, TxUtil.txUtilInstance.getRandomKey(aRegion));
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_DESTROY)) {
         Region aRegion = TxUtil.txUtilInstance.getRandomRegion(true);
         op = getDestroyEntryOp(false, aRegion, TxUtil.txUtilInstance.getRandomKey(aRegion));
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_LOCAL_DESTROY)) {
         Region aRegion = TxUtil.txUtilInstance.getRandomRegion(true);
         op = getDestroyEntryOp(true, aRegion, TxUtil.txUtilInstance.getRandomKey(aRegion));
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_INVAL)) {
         Region aRegion = TxUtil.txUtilInstance.getRandomRegion(true);
         op = getInvalEntryOp(false, aRegion, TxUtil.txUtilInstance.getRandomKey(aRegion));
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_LOCAL_INVAL)) {
         Region aRegion = TxUtil.txUtilInstance.getRandomRegion(true);
         op = getInvalEntryOp(true, aRegion, TxUtil.txUtilInstance.getRandomKey(aRegion));
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_GET_NEW_KEY)) {
         Region aRegion = TxUtil.txUtilInstance.getRandomRegion(true);
         op = getGetEntryWithNewKeyOp(aRegion);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_GET_EXIST_KEY)) {
         Region aRegion = TxUtil.txUtilInstance.getRandomRegion(true);
         op = getGetEntryWithExistingKeyOp(aRegion);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_GET_PREV_KEY)) {
         Region aRegion = TxUtil.txUtilInstance.getRandomRegion(true);
         op = getGetEntryWithPreviousKeyOp(aRegion);
      } else if (operation.equalsIgnoreCase(Operation.REGION_CREATE)) {
         op = getCreateRegionOp();
      } else if (operation.equalsIgnoreCase(Operation.REGION_DESTROY)) {
         op = getDestroyRegionOp(false, TxUtil.txUtilInstance.getRandomRegion(false));
      } else if (operation.equalsIgnoreCase(Operation.REGION_LOCAL_DESTROY)) {
         op = getDestroyRegionOp(true, TxUtil.txUtilInstance.getRandomRegion(false));
      } else if (operation.equalsIgnoreCase(Operation.REGION_INVAL)) {
         Region aRegion = TxUtil.txUtilInstance.getRandomRegion(true);
         op = getInvalRegionOp(false, TxUtil.txUtilInstance.getRandomRegion(false));
      } else if (operation.equalsIgnoreCase(Operation.REGION_LOCAL_INVAL)) {
         op = getInvalRegionOp(true, TxUtil.txUtilInstance.getRandomRegion(false));
      } else { // unknown operation
        throw new TestException("Unknown operation " + operation);
      }
      attempts++;
      if (attempts > 20) {
        throw new TestException("Could not execute an operation in 20 attempts (perhaps no keys exist).  Check client vm logs for details");
      }
    }
    Log.getLogWriter().info("getOperation() returning " + op.toString());
    return op;
  }

  // ======================================================================== 
  // methods to get Operations on region entries
  // Note that the operations are not performed but an Operation 
  // instance is created that describes the operation to be performed
  // ======================================================================== 

  /** 
   *  getCreateEntryOp()
   *
   *  @param aRegion - entry op describes a create-entry op in the given region
   *
   *  @return Operation op - a createEntry Operation 
   */
  public Operation getCreateEntryOp(Region aRegion) {
     Object key = NameFactory.getNextPositiveObjectName();
     BaseValueHolder vh = new ValueHolder(key, randomValues, new Integer(modValInitializer++));
     return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_CREATE, null, vh);
  }
  
  /** 
   *  getUpdateEntryOp()
   *  
   *  @param aRegion The region to modify a key in.
   *  @param key The key to modify.
   *
   *  @return An instance of Operation describing the update operation.
   */
  public Operation getUpdateEntryOp(Region aRegion, Object key) {
     if (key == null) {
        TxUtil.txUtilInstance.createEntries(aRegion);
        return null;
     }

     // get the old value
     BaseValueHolder vh = null;
  
     Object oldValue = getValueInVM(aRegion, key);
     if (oldValue instanceof BaseValueHolder) { // cannot update this key
        vh = (BaseValueHolder)oldValue;
     } else {                          // null, INVALID, LOCAL_INVALID
        vh = new ValueHolder(key, randomValues, new Integer(modValInitializer++));
     }

     // MUST copy (as getValueInVM() not affected by cache.setCopyOnRead setting)
     vh = (BaseValueHolder)CopyHelper.copy(vh);
     vh.modVal = new Integer(vh.modVal.intValue() + 1);
     return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_UPDATE, oldValue, vh);
  }

  /** 
   *  getDestroyEntryOp()
   * 
   *  @param isLocalDestroy True if the opertion is a local destroy, false otherwise.
   *  @param aRegion The region to destroy the entry in.
   *  @param key The key to destroy.
   *
   *  @return An instance of Operation describing the destroy operation.
   */
  public Operation getDestroyEntryOp(boolean isLocalDestroy, Region aRegion, Object key) {
     if (key == null) {
        TxUtil.txUtilInstance.createEntries(aRegion);
        return null;
     }

     Object oldValue = getValueInVM(aRegion, key);
  
     if (isLocalDestroy) {
        return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_LOCAL_DESTROY, oldValue, null);
     } else {
        return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_DESTROY, oldValue, null);
     }
  }

  /** getInvalEntryOp()
   *  
   *  @param isLocalInval True if the opertion is a local invalidate, false otherwise.
   *  @param aRegion The region to invalidate the entry in.
   *  @param key The key to invalidate.
   *
   *  @return An instance of Operation describing the invalidate operation.
   */
  public Operation getInvalEntryOp(boolean isLocalInval, Region aRegion, Object key) {
     if (key == null) {
        TxUtil.txUtilInstance.createEntries(aRegion);
        return null;
     }
  
     Object oldValue = getValueInVM(aRegion, key);
  
     if (isLocalInval) {
        return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_LOCAL_INVAL, oldValue, null);
     } else {
        return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_INVAL, oldValue, null);
     }
  }

  /** getGetEntryWithExistingKeyOp()
   *  
   *  @param aRegion The region to get from.
   *
   *  @return An instance of Operation describing the get operation.
   *
   *  Note: operation.newVal = null upon return
   */
  public Operation getGetEntryWithExistingKeyOp(Region aRegion) {
     Object key = TxUtil.txUtilInstance.getRandomKey(aRegion);
     if (key == null) {
        TxUtil.txUtilInstance.createEntries(aRegion);
        return null;
     }
  
     Object oldValue = getValueInVM(aRegion, key);
     return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_GET_EXIST_KEY, oldValue, null);
  }

  /** getGetEntryWithPreviousKey()
   *  
   *  @param aRegion The region to get from.
   *
   *  @return An instance of Operation describing the get operation.
   *  Note: operation.newVal = null upon return
   */
  public Operation getGetEntryWithPreviousKeyOp(Region aRegion) {
     long keysUsed = NameFactory.getPositiveNameCounter();
     Object key = NameFactory.getObjectNameForCounter(TestConfig.tab().getRandGen().nextInt(1, (int)keysUsed));
  
     Object oldValue = getValueInVM(aRegion, key);
  
     return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_GET_PREV_KEY, oldValue, null);
  }

  /** getGetEntryWithNewKey()
   *  
   *  @param aRegion The region to get from.
   *
   *  @return An instance of Operation describing the get operation.
   *  Note: operation.newVal = null upon return
   */
  public Operation getGetEntryWithNewKeyOp(Region aRegion) {
     Object key = NameFactory.getNextPositiveObjectName();
  
     Object oldValue = getValueInVM(aRegion, key);
  
     return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_GET_NEW_KEY, oldValue, null);
  }

  // ======================================================================== 
  // methods to get a projected region operation 
  // ======================================================================== 

  /** getInvalRegionOp()
   *  
   *  @param isLocalInval True if the opertion is a local invalidate, false otherwise.
   *  @param aRegion The region to invalidate.
   *
   *  @return An instance of Operation describing the invalidate operation.
   */
  public Operation getInvalRegionOp(boolean isLocalInval, Region aRegion) {
    if (isLocalInval) {
      return new Operation(aRegion.getFullPath(), null, Operation.REGION_LOCAL_INVAL, null, null);
    } else {
      return new Operation(aRegion.getFullPath(), null, Operation.REGION_INVAL, null, null);
    }
  }
  
  /** 
   *  getDestroyRegionOp()
   *
   *  @param isLocalDestroy True if the opertion is a local destroy, false otherwise.
   *  @param aRegion The region to destroy.
   *
   *  @return An instance of Operation describing the destroy operation.
   */
  public Operation getDestroyRegionOp(boolean isLocalDestroy, Region aRegion) {
     if (aRegion == null) {
       throw new TestException("No regions left to destroy\n" + TestHelper.getStackTrace());
     }
  
     if (isLocalDestroy) {
        return new Operation(aRegion.getFullPath(), null, Operation.REGION_LOCAL_DESTROY, null, null);
     } else {
        return new Operation(aRegion.getFullPath(), null, Operation.REGION_DESTROY, null, null);
     }
  }

  /** getCreateRegionOp()
   *  
   *  @return An instance of Operation describing the create operation.
   *          returns 'null' if no regions previously destroyed
   */
  public Operation getCreateRegionOp() {
     // Select a random region to work with from the list of destroyed regions
     ArrayList list = (ArrayList)(TxBB.getBB().getSharedMap().get(TxBB.DestroyedRegionsKey));
     if (list.size() > 0) {
       int index = TestConfig.tab().getRandGen().nextInt(0, list.size()-1);
       String regionPath = (String)(list.get(index));
       return new Operation(regionPath, null, Operation.REGION_CREATE, null, null);
     } else {
       return null;
     }
  }

  public Operation executeOperation(Operation operation) {

    String opName = operation.getOpName();
    Operation op;

    if (opName.equalsIgnoreCase(Operation.ENTRY_CREATE)) {
       op = executeCreateEntry(operation);
    } else if (opName.equalsIgnoreCase(Operation.ENTRY_UPDATE)) {
       op = executeUpdateEntry(operation);
    } else if (opName.equalsIgnoreCase(Operation.ENTRY_DESTROY)) {
       op = executeDestroyEntry(operation);
    } else if (opName.equalsIgnoreCase(Operation.ENTRY_LOCAL_DESTROY)) {
       op = executeDestroyEntry(operation);
    } else if (opName.equalsIgnoreCase(Operation.ENTRY_INVAL)) {
       op = executeInvalEntry(operation);
    } else if (opName.equalsIgnoreCase(Operation.ENTRY_LOCAL_INVAL)) {
       op = executeInvalEntry(operation);
    } else if (opName.equalsIgnoreCase(Operation.ENTRY_GET_NEW_KEY)) {
       op = executeGetEntryWithNewKey(operation);
    } else if (opName.equalsIgnoreCase(Operation.ENTRY_GET_EXIST_KEY)) {
       op = executeGetEntryWithExistingKey(operation);
    } else if (opName.equalsIgnoreCase(Operation.ENTRY_GET_PREV_KEY)) {
       op = executeGetEntryWithPreviousKey(operation);
    } else if (opName.equalsIgnoreCase(Operation.REGION_CREATE)) {
       op = executeCreateRegion(operation);
    } else if (opName.equalsIgnoreCase(Operation.REGION_DESTROY)) {
       op = executeDestroyRegion(operation);
    } else if (opName.equalsIgnoreCase(Operation.REGION_LOCAL_DESTROY)) {
       op = executeDestroyRegion(operation);
    } else if (opName.equalsIgnoreCase(Operation.REGION_INVAL)) {
       op = executeInvalRegion(operation);
    } else if (opName.equalsIgnoreCase(Operation.REGION_LOCAL_INVAL)) {
       op = executeInvalRegion(operation);
    } else { // unknown operation
      throw new TestException("Unknown operation " + operation);
    }
    return op;
  }

  // ======================================================================== 
  // methods to execute operations on region entries
  // Given a previously constructed operation (previously constructed), 
  // performs the desired operation.
  // ======================================================================== 
  
  /** Creates a new key/value in the given region by creating a new
   *  (never-used-before) key and a random value.
   *  
   *  @param Operation op - describes operation to be performed
   * 
   *  @return Operation op - completed op 
   */
  public Operation executeCreateEntry(Operation op) {
  
     String regionName = op.getRegionName();
     Object key = (String)op.getKey();
     Object vh = op.getNewValue();
     Region aRegion = CacheUtil.getCache().getRegion(regionName);
  
     try {
        Log.getLogWriter().info("createEntry: putting key " + key + ", object " + vh.toString() + " in region " + regionName);
        aRegion.put(key, vh);
        Log.getLogWriter().info("createEntry: done putting key " + key + ", object " + vh.toString() + " in region " + regionName);
     } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
     }
     return op;
  }

  /** Updates an existing entry in aRegion. The value in the key
   *  is increment by 1.
   *  
   *  @param Operation op - describes operation to be performed
   *
   *  @return An instance of Operation describing the update operation.
   */
  public Operation executeUpdateEntry(Operation op) {
  
     String regionName = op.getRegionName();
     Object key = (String)op.getKey();
     Object vh = op.getNewValue();
     Region aRegion = CacheUtil.getCache().getRegion(regionName);
  
     if (key == null) {
        Log.getLogWriter().info("Could not update a key in " + regionName + " because no keys are available");
        return null;
     }
  
     try {
        Log.getLogWriter().info("updateEntry: Putting new value " + vh + " for key " + key + " in region " + regionName);
        aRegion.put(key, vh);
        Log.getLogWriter().info("updateEntry: Done putting new value " + vh + " for key " + key + " in region " + regionName);
     } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
     }
     return op;
  }

  /** Destroys an entry in aRegion. 
   *  
   *  @param Operation op - describes operation to be performed
   *
   *  @return An instance of Operation describing the destroy operation.
   */
  public Operation executeDestroyEntry(Operation op) {
  
     String regionName = op.getRegionName();
     Object key = op.getKey();
     Region aRegion = CacheUtil.getCache().getRegion(regionName);
  
     if (key == null) {
        Log.getLogWriter().info("Could not destroy an entry in " + regionName + " because no keys are available");
        return null;
     }

     try {
        if (op.isLocalOperation()) {
           Log.getLogWriter().info("destroyEntry: locally destroying key " + key + " in region " + regionName);
           aRegion.localDestroy(key);
           Log.getLogWriter().info("destroyEntry: done locally destroying key " + key + " in region " + regionName);
        } else {
           Log.getLogWriter().info("destroyEntry: destroying key " + key + " in region " + regionName);
           aRegion.destroy(key);
           Log.getLogWriter().info("destroyEntry: done destroying key " + key + " in region " + regionName);
        }
     } catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     } catch (EntryNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     } catch (CacheWriterException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     }
     return op;
  }

  /** Invalidates an entry in aRegion. 
   *  
   *  @param Operation op - describes operation to be performed
   *
   *  @return An instance of Operation describing the invalidate operation.
   */
  public Operation executeInvalEntry(Operation op) {
  
     String regionName = op.getRegionName();
     Object key = op.getKey();
     Region aRegion = CacheUtil.getCache().getRegion(regionName);

     if (key == null) { 
       Log.getLogWriter().info("Could not invalidate an entry in " + regionName + " because no keys are available");
        return null;
     }

     try {
        if (op.isLocalOperation()) {
           Log.getLogWriter().info("invalEntry: locally invalidating key " + key + " in region " + regionName);
           aRegion.localInvalidate(key);
           Log.getLogWriter().info("invalEntry: done locally invalidating key " + key + " in region " + regionName);
        } else {
           Log.getLogWriter().info("invalEntry: invalidating key " + key + " in region " + regionName);
           aRegion.invalidate(key);
           Log.getLogWriter().info("invalEntry: done invalidating key " + key + " in region " + regionName);
        }
     } catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     } catch (EntryNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     }

     Object newValue = getValueInVM(aRegion, key);
     op.setNewValue(newValue);
     return op;
  }

  /** Gets a value in aRegion with a key existing in aRegion.
   *  
   *  @param Operation op - describes operation to be performed
   * 
   *  @return Operation op - updated operation
   *
   */
  public Operation executeGetEntryWithExistingKey(Operation op) {
  
     String regionName = op.getRegionName();
     Object key = op.getKey();
     Region aRegion = CacheUtil.getCache().getRegion(regionName);
  
     if (key == null) {
        Log.getLogWriter().info("Could not get with an existing key " + regionName + " because no keys are available");
        return null;
     }
  
     try {
        Log.getLogWriter().info("getEntryWithExistingKey: getting value for key " + key + " in region " + regionName);
        Object newValue = aRegion.get(key);
        Log.getLogWriter().info("getEntryWithExistingKey: got value for key " + key + ": " + newValue + " in region " + regionName);
        op.setNewValue(newValue);
     } catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     } catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     }
     return op;
  }

  /** Gets a value in aRegion with a random key that was previously used in the test.
   *  
   *  @param Operation op - describes operation to be performed
   *
   *  @return An instance of Operation describing the get operation.
   */
  public Operation executeGetEntryWithPreviousKey(Operation op) {
  
     String regionName = op.getRegionName();
     Object key = op.getKey();
     Region aRegion = CacheUtil.getCache().getRegion(regionName);
  
     try {
        Object oldValue = getValueInVM(aRegion, key);
        Log.getLogWriter().info("getEntryWithPreviousKey: getting value for key " + key + " in region " + regionName);
        Object newValue = aRegion.get(key);
        Log.getLogWriter().info("getEntryWithPreviousKey: got value for key " + key + ": " + newValue + " in region " + regionName);
        op.setNewValue(newValue);
     } catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     } catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     }
     return op;
  }

  /** Gets a value in aRegion with a the next new (never-before_used) key.
   *  
   *  @param Operation op - describes operation to be performed
   *
   *  @return An instance of Operation describing the get operation.
   */
  public Operation executeGetEntryWithNewKey(Operation op) {
     String regionName = op.getRegionName();
     Object key = op.getKey();
     Region aRegion = CacheUtil.getCache().getRegion(regionName);
  
     try {
        Log.getLogWriter().info("getEntryWithNewKey: getting value for key " + key + " in region " + regionName);
        Object newValue = aRegion.get(key);
        Log.getLogWriter().info("getEntryWithNewKey: got value for key " + key + ": " + newValue + " in region " + regionName);
        op.setNewValue(newValue);
     } catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     } catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     }
     return op;
  }

  // ======================================================================== 
  // methods to execute operations on regions
  // ======================================================================== 
  
  /** Invalidates the given region. 
   *  
   *  @param Operation op - describes operation to be performed
   *
   *  @return An instance of Operation describing the invalidate operation.
   */
  public Operation executeInvalRegion(Operation op) {
  
     String regionName = op.getRegionName();
     Region aRegion = CacheUtil.getCache().getRegion(regionName);
  
     try {
        if (op.isLocalOperation()) {
           Log.getLogWriter().info("invalRegion: locally invalidating region " + regionName);
           aRegion.localInvalidateRegion();
           Log.getLogWriter().info("invalRegion: done locally invalidating region " + aRegion.getFullPath());
        } else {
           Log.getLogWriter().info("invalRegion: invalidating region " + regionName);
           aRegion.invalidateRegion();
           Log.getLogWriter().info("invalRegion: done invalidating region " + regionName);
        }
     } catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     }
     return op;
  }

/** Destroys the given region. 
 *  
 *  @param Operation op - describes operation to be performed
 *
 *  @return An instance of Operation describing the destroy operation.
 */
public Operation executeDestroyRegion(Operation op) {

   String regionName = op.getRegionName();
   Region aRegion = CacheUtil.getCache().getRegion(regionName);

   if (aRegion == null) {
     throw new TestException("No regions left to destroy\n" + TestHelper.getStackTrace());
   }

   try {
      TxUtil.txUtilInstance.recordDestroyedRegion(aRegion);
      if (op.isLocalOperation()) {
         Log.getLogWriter().info("destroyRegion: locally destroying region " + regionName);
         aRegion.localDestroyRegion();
         Log.getLogWriter().info("destroyRegion: done locally destroying region " + regionName);
      } else {
         Log.getLogWriter().info("destroyRegion: destroying region " + regionName);
         aRegion.destroyRegion();
         Log.getLogWriter().info("destroyRegion: done destroying region " + regionName);
      }
   } catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   return op;
}

  private void validateInTx(Operation op, CollectionInfo originalData, CollectionInfo txData) {
    Log.getLogWriter().info("validateInTx: op = " + op.toString());
   
    String opName = op.getOpName();
    Object key = op.getKey();
    Object oldValue = op.getOldValue();
    Object newValue = op.getNewValue();
    String regionName = op.getRegionName();

    // we cannot access keySet, entrySet, values or related iterators that were obtained outside of the transaction
    verifyCollectionsNotAccessible(originalData, op, false); 

    Log.getLogWriter().info("validateInTx:  Region collections and iterators obtained outside of transaction properly threw IllegalStateException when accessed within Tx, continuing test");

    Region aRegion = CacheUtil.getCache().getRegion(regionName);

    // For PartitionedRegions the keySet, values, entrySet and Region.Entry
    // are not backed by the local region as they are for Distributed Regions
    if (TxPrms.isPartitionedRegion()) {
      return;
    }

    if (opName.equalsIgnoreCase(Operation.ENTRY_CREATE)) {

      // we should be able to find the new entry in the new Collections
      verifyKeys(txData, key, true);
      verifyValues(txData, newValue, true);
      verifyEntries(txData, key, newValue, true);
 
      // original entry == null; tx entry == (key, newValue) pair
      if (!txData.entry.getValue().equals(aRegion.getEntry(key).getValue())) {
        throw new TestException("validateInTransaction failure: region.getEntry() value doesn't match getEntry value from TxData, txData entry = (key=" + txData.entry.getKey() + ", value = " + txData.entry.getValue() + " vs region.getEntry(key).getValue() = " + aRegion.getEntry(key).getValue() + ")");
      }

    } else if (opName.equalsIgnoreCase(Operation.ENTRY_UPDATE)) {

        // we should be able to find key & newValue in the new Collection
        verifyKeys(txData, key, true);
        verifyValues(txData, newValue, true);
        verifyEntries(txData, key, newValue, true);

        // original entry == (key, oldValue)
        if (op.isPreviouslyInvalid()) {
          if (originalData.entry.getValue() != null) {
            throw new TestException("validateInTransaction failure: originalData.entry.getValue() should be null, but is " + originalData.entry.getValue());
          }
        } else if (originalData.entry.getValue().equals(aRegion.getEntry(key).getValue())) {
          throw new TestException("validateInTransaction failure: region.getEntry() value matches getEntry object from originalData, but shouldn't: originalData entry = (key=" + originalData.entry.getKey() + ", value = " + originalData.entry.getValue() + " vs region.getEntry(key).getValue() = " + aRegion.getEntry(key).getValue() + ")");
        }
 
        // tx entry == (key, newValue) pair
        if (!txData.entry.getValue().equals(aRegion.getEntry(key).getValue())) {
          throw new TestException("validateInTransaction failure: region.getEntry() value doesn't match getEntry value from TxData, txData entry = (key=" + txData.entry.getKey() + ", value = " + txData.entry.getValue() + " vs region.getEntry(key).getValue() = " + aRegion.getEntry(key).getValue() + ")");
        }
    } else if ((opName.equalsIgnoreCase(Operation.ENTRY_DESTROY)) ||
               (opName.equalsIgnoreCase(Operation.ENTRY_LOCAL_DESTROY))) {

        // we should NOT be able to find key & newValue in the new Collection
        verifyKeys(txData, key, false);
        verifyValues(txData, newValue, false);
        verifyEntries(txData, key, newValue, false);
 
        // original entry == (key, oldValue) 
        if (aRegion.getEntry(key) != null) {
          throw new TestException("validateInTransaction failure: getEntry() should be null (entryDestroyed), but is " + aRegion.getEntry(key));
        }

        // tx entry == null
        if (txData.entry != null) {
          throw new TestException("validateInTransaction failure: txData.entry should show isDestroyed()");
        }
    } else if ((opName.equalsIgnoreCase(Operation.ENTRY_INVAL)) ||
               (opName.equalsIgnoreCase(Operation.ENTRY_LOCAL_INVAL))) {

        // we should be able to find key & newValue in the new Collection
        // Note newValue == INVALID, but in collection it will be null
        verifyKeys(txData, key, true);
        verifyValues(txData, null, false);
        verifyEntries(txData, key, null, true);
 
        // original entry == (key, oldValue)
        // be careful if previouslyInvalidated
        if (op.isPreviouslyInvalid()) {
          if (originalData.entry.getValue() != null) {
            throw new TestException("validateInTransaction failure: entry (" + key + ") previously invalidated, but originalData.entry.getValue() != null, instead found " + originalData.entry.getValue());
          }
        } else if (aRegion.getEntry(key).getValue() != null) {
          throw new TestException("validateInTransaction failure: expected Region.getEntry(key).getValue() == null (after invalidate), but found " + aRegion.getEntry(key).getValue());
        }

        // tx entry == (key, invalidated) 
        // note that what we'll really find in the collection and get back 
        // from the region.getEntry(key).getValue is 'null' (don't use .equals).
        Object txEntry = txData.entry.getValue();
        Object regionEntry = aRegion.getEntry(key).getValue();
        if (!(txEntry == regionEntry)) {
          throw new TestException("validateInTransaction failure: region.getEntry() value doesn't match getEntry value from TxData, txData entry = (key=" + txData.entry.getKey() + ", value = " + txEntry + " vs region.getEntry(key).getValue() = " + regionEntry);
        }
    } else if (opName.equalsIgnoreCase(Operation.ENTRY_GET_NEW_KEY)) {

        // we should be able to find key & newValue in the new Collection
        // because loads now go directly to tx state
        verifyKeys(txData, key, true);
        verifyValues(txData, newValue, true);
        verifyEntries(txData, key, newValue, true);
 
        // we can't check the original value as it didn't yet exist before the tx!
        // tx entry == (key, newValue) pair
        if (!txData.entry.getValue().equals(aRegion.getEntry(key).getValue())) {
          throw new TestException("validateInTransaction failure: entry for key " + key + " from txData was not updated with newly loaded value");
        }
    } else if (opName.equalsIgnoreCase(Operation.ENTRY_GET_EXIST_KEY)) {

        // we should be able to find key & newValue in the new Collection
        verifyKeys(txData, key, true);
        verifyValues(txData, newValue, true);
        verifyEntries(txData, key, newValue, true);

        // if op was invalid and we are still in the transaction, the
        // newly loaded value will only be in txState (not committed state)
        // otherwise originalData.entry.getValue() should be == oldValue
        if (op.isPreviouslyInvalid()) {
          if (originalData.entry.getValue() != null) {
            throw new TestException("validateInTransaction failure: previously invalidated entry, originalData.entry.getValue() should be null, but is " + originalData.entry.getValue());
          }
        } else if (!originalData.entry.getValue().equals(oldValue)) {
          throw new TestException("validateInTransaction failure: after getWithExistingKey, expected originalData.entry.getValue() == oldValue, but found originalData = " + originalData.entry.getValue() + " with oldValue = " + oldValue);
        }
 
    } else if (opName.equalsIgnoreCase(Operation.ENTRY_GET_PREV_KEY)) {

        // we should be able to find key & newValue in the new Collection
        verifyKeys(txData, key, true);
        verifyValues(txData, newValue, true);
        verifyEntries(txData, key, newValue, true);
 
        // tx entry == (key, newValue) pair
        if (!txData.entry.getValue().equals(aRegion.getEntry(key).getValue())) {
          throw new TestException("validateInTransaction failure: region.getEntry() value doesn't match getEntry value from TxData, txData entry = (key=" + txData.entry.getKey() + ", value = " + txData.entry.getValue() + " vs region.getEntry(key).getValue() = " + aRegion.getEntry(key).getValue() + ")");
        }
    } else if (opName.equalsIgnoreCase(Operation.REGION_CREATE)) {
        checkRegionData(originalData, txData, op, ONE_MORE);
    } else if (opName.equalsIgnoreCase(Operation.REGION_DESTROY)) {
        checkRegionData(originalData, txData, op, ONE_LESS);
    } else if (opName.equalsIgnoreCase(Operation.REGION_LOCAL_DESTROY)) {
        checkRegionData(originalData, txData, op, ONE_LESS);
    } else if (opName.equalsIgnoreCase(Operation.REGION_INVAL)) {
        checkRegionData(originalData, txData, op, EQUAL);
    } else if (opName.equalsIgnoreCase(Operation.REGION_LOCAL_INVAL)) {
        checkRegionData(originalData, txData, op, EQUAL);
    } else { // unknown operation
      throw new TestException("Unknown operation " + op);
    }
    Log.getLogWriter().info("validateInTx: data and collection behavior validation successful");
  }

  // Original collections should be accessible now (with updated region contents)
  // collections/iterator obtained with the tx should NOT be accessible
  private void validateAfterCommit(Operation op, CollectionInfo originalData, CollectionInfo txData) {
    Log.getLogWriter().info("validateAfterCommit, op = " + op.toString());
   
    String opName = op.getOpName();
    Object key = op.getKey();
    Object oldValue = op.getOldValue();
    Object newValue = op.getNewValue();
    String regionName = op.getRegionName();

    // we cannot access keySet, entrySet, or values sets that were obtained within the transaction
    verifyCollectionsNotAccessible(txData, op, true);

    if (!op.isEntryOperation()) {
      // for regionOperations, verify that we cannot access subregions Collections from within tx
      verifySubregionsSetAccessible(txData);
    }

    Log.getLogWriter().info("validateAfterCommit:  Region collections and iterators obtained within transaction properly threw IllegalStateException when accessed after commit, continuing test");

    Region aRegion = CacheUtil.getCache().getRegion(regionName);

    // For PartitionedRegions the keySet, values, entrySet and Region.Entry
    // are not backed by the local region as they are for Distributed Regions
    if (PartitionRegionHelper.isPartitionedRegion(aRegion)) {
      return;
    }

    if (opName.equalsIgnoreCase(Operation.ENTRY_CREATE)) {
      // updates should now be seen in our original collection
      verifyKeys(originalData, key, true);
      verifyValues(originalData, newValue, true);
      verifyEntries(originalData, key, newValue, true);

    } else if (opName.equalsIgnoreCase(Operation.ENTRY_UPDATE)) {
      // updates should now be seen in our original collection
      verifyKeys(originalData, key, true);
      verifyValues(originalData, newValue, true);
      verifyEntries(originalData, key, newValue, true);

      if (!originalData.entry.getValue().equals(aRegion.getEntry(key).getValue())) { 
        throw new TestException("validateInTransaction failure: region.getEntry() value doesn't match getEntry value from originalData, originalData entry = (key=" + originalData.entry.getKey() + ", value = " + originalData.entry.getValue() + " vs region.getEntry(key).getValue() = " + aRegion.getEntry(key).getValue() + ")");
      }

    } else if ((opName.equalsIgnoreCase(Operation.ENTRY_DESTROY)) ||
               (opName.equalsIgnoreCase(Operation.ENTRY_LOCAL_DESTROY))) {
      // we should NOT be able to find key in the originalCollection
      verifyKeys(originalData, key, false);
      verifyValues(originalData, oldValue, false);
      verifyEntries(originalData, key, oldValue, false);                                                                                
      // original entry == (key, oldValue)
      if (aRegion.getEntry(key) != null) {
        throw new TestException("validateInTransaction failure: getEntry() should be null (entryDestroyed), but is " + aRegion.getEntry(key));
      }

    } else if ((opName.equalsIgnoreCase(Operation.ENTRY_INVAL)) ||
               (opName.equalsIgnoreCase(Operation.ENTRY_LOCAL_INVAL))) {

      // key is in our original collection w/ newValue (invalidated)
      verifyKeys(originalData, key, true);
      verifyValues(originalData, newValue, true);
      verifyEntries(originalData, key, newValue, true);
                                                                               
      // original entry == (key, null) [ INVALIDATED ]
      if (originalData.entry.getValue() != null) {
        throw new TestException("validateInTransaction failure: region.getEntry() value matches getEntry object from originalData, but shouldn't: originalData entry = (key=" + originalData.entry.getKey() + ", value = " + originalData.entry.getValue() + " vs region.getEntry(key).getValue() = " + aRegion.getEntry(key).getValue() + ")");
      }
                                                                               
    } else if (opName.equalsIgnoreCase(Operation.ENTRY_GET_NEW_KEY)) {

      // should see new key & value in our committed state
      verifyKeys(originalData, key, true);
      verifyValues(originalData, newValue, true);
      verifyEntries(originalData, key, newValue, true);
                                                                               
    } else if (opName.equalsIgnoreCase(Operation.ENTRY_GET_EXIST_KEY)) {
      // key is in our original collection w/ oldValue
      verifyKeys(originalData, key, true);
      verifyValues(originalData, newValue, true);
      verifyEntries(originalData, key, newValue, true);

      // original entry == (key, newValue) pair         
      if (!originalData.entry.getValue().equals(aRegion.getEntry(key).getValue())) {
          throw new TestException("validateInTransaction failure: region.getEntry() value doesn't match getEntry value from originalData, originalData entry = (key=" + originalData.entry.getKey() + ", value = " + originalData.entry.getValue() + " vs region.getEntry(key).getValue() = " + aRegion.getEntry(key).getValue() + ")");         
      }

    } else if (opName.equalsIgnoreCase(Operation.ENTRY_GET_PREV_KEY)) {

      // we should be able to find key & newValue in updated original collection
      verifyKeys(originalData, key, true);
      verifyValues(originalData, newValue, true);
      verifyEntries(originalData, key, newValue, true);

      // original entry == (key, newValue) pair
      if (originalData.entry != null) {
        if (!originalData.entry.getValue().equals(aRegion.getEntry(key).getValue())) {
          throw new TestException("validateInTransaction failure: region.getEntry() value doesn't match getEntry value from originalData, originalData entry = (key=" + originalData.entry.getKey() + ", value = " + originalData.entry.getValue() + " vs region.getEntry(key).getValue() = " + aRegion.getEntry(key).getValue() + ")");
        }
      }
    } else if (opName.equalsIgnoreCase(Operation.REGION_CREATE)) {
        // should find our new region in the subRegions data
        boolean found = false;
        for (Iterator it=originalData.subRegions.iterator(); it.hasNext(); ) {
          Region oRegion = (Region)it.next();
          if (oRegion.getFullPath().equals(op.getRegionName())) {
            found = true;
          }
        }
        if (!found) {
          throw new TestException("validateAfterCommit validation failure: did NOT find regionName " + op.getRegionName() + " as expected after op = " + op.toString());
        }
    } else if ((opName.equalsIgnoreCase(Operation.REGION_DESTROY)) ||
               (opName.equalsIgnoreCase(Operation.REGION_LOCAL_DESTROY))) {
        // iterate (should NOT find our new region in originalData)
        for (Iterator it=originalData.subRegions.iterator(); it.hasNext(); ) {
          Region oRegion = (Region)it.next();
          if (oRegion.getFullPath().equals(op.getRegionName())) {
            throw new TestException("checkRegionData validationfailure: found regionName " + op.getRegionName() + ": not expected after op = " + op.toString());
          }
        }

    } else if ((opName.equalsIgnoreCase(Operation.REGION_INVAL)) ||
               (opName.equalsIgnoreCase(Operation.REGION_LOCAL_INVAL))) {
        // should find our new region in the subRegions data
        boolean found = false;
        for (Iterator it=originalData.subRegions.iterator(); it.hasNext(); ) {
          Region oRegion = (Region)it.next();
          if (oRegion.getFullPath().equals(op.getRegionName())) {
            found = true;
            // entries should all be invalid
            Collection values = oRegion.values();
            for (Iterator v=values.iterator(); v.hasNext(); ) {
              Object o = v.next();
              if (o != null) {
                throw new TestException("validateAfterCommit validation failure: found non-null value " + o + " in invalidated region " + oRegion.getFullPath());
              }
            }
          }
        }
        if (!found) {
          throw new TestException("validateAfterCommit validationfailure: did NOT find regionName " + op.getRegionName() + " as expected after op = " + op.toString());
        }

    } else { // unknown operation
      throw new TestException("Unknown operation " + op);
    }
    Log.getLogWriter().info("validateAfterCommit: data and collection behavior validation successful");
  }

  // Region operations are immediate and go directly to committed & txState!
  // Therefore, all RegionCollections will incur like changes in both original
  // and txData!

  // Expectations for difference between oData & txData
  private static final int ONE_MORE  = 0;    // regionCreate
  private static final int ONE_LESS  = 1;    // regionDestroy
  private static final int EQUAL     = 2;    // regionInvalidate

  private void checkRegionData(CollectionInfo originalData, CollectionInfo txData, Operation op, int expected) {
 
    // check for same parentRegionName ... if good, verify subRegions size as expected
    if (!originalData.parentRegionName.equals(txData.parentRegionName)) {
      throw new TestException("checkRegionData validation failure: original & txdata have different parentRegionNames, originalData = " + originalData.parentRegionName + " txData = " + txData.parentRegionName);
    }

    boolean found;
    // There should be no difference in subregions.size() -- changes affected both
    // committed & txState
    int diff = originalData.subRegions.size() - txData.subRegions.size();
    if (diff != 0) {
      throw new TestException("checkRegionData validation failure: region operations are immediate and affect both committed & transactional state.  Region,subregions() should return the same number of subRegions for both original & txData");
    }

    switch (expected) {
      case ONE_MORE:                      // region create
      case EQUAL:                         // region invalidate
        // region should be found in original & tx subRegions collections
        found = false;
        for (Iterator it=originalData.subRegions.iterator(); it.hasNext(); ) {
          Region aRegion = (Region)it.next();
          if (aRegion.getFullPath().equals(op.getRegionName())) {
            found = true;
          }
        }
        if (!found) {
          throw new TestException("checkRegionData validationfailure: did NOT find regionName " + op.getRegionName() + " in originalData " + originalData.subRegions.toString() + " as expected after op = " + op.toString());
        }
        // iterate (should find our new region in txData)
        found = false;
        for (Iterator it=txData.subRegions.iterator(); it.hasNext(); ) {
          Region aRegion = (Region)it.next();
          if (aRegion.getFullPath().equals(op.getRegionName())) {
            found = true;
          }
        }
        if (!found) {
          throw new TestException("checkRegionData validationfailure: did NOT find regionName " + op.getRegionName() + " in txData " + txData.subRegions.toString() + " as expected after op = " + op.toString());
        }
        break;
      case ONE_LESS:                      // destroyRegion
        // iterate (should NOT find our new region in originalData)
        for (Iterator it=originalData.subRegions.iterator(); it.hasNext(); ) {
          Region aRegion = (Region)it.next();
          if (aRegion.getFullPath().equals(op.getRegionName())) { 
            throw new TestException("checkRegionData validationfailure: found regionName " + op.getRegionName() + ": not expected after op = " + op.toString());
          }
        }
        // iterate (should NOT find our new region in originalData)
        for (Iterator it=txData.subRegions.iterator(); it.hasNext(); ) {
          Region aRegion = (Region)it.next();
          if (aRegion.getFullPath().equals(op.getRegionName())) {
            throw new TestException("checkRegionData validationfailure: found regionName " + op.getRegionName() + ": not expected after op = " + op.toString());
          }
        }
        break;
    }  // switch
  }

  private void verifyKeys(CollectionInfo data, Object key, boolean expected) {
    boolean found = false;
    for (Iterator it=data.keys.iterator(); it.hasNext(); ) {
      if (it.next().equals(key)) {
        found = true;
      }
    }
    if (found != expected) {
      throw new TestException("CollectionValidation failure: verifyKeys: key found = " + found + " expected = " + expected);
    }
  }

  private void verifyValues(CollectionInfo data, Object value, boolean expected) {

    // caveat - if the value was INVALID or LOCAL_INVALID, we won't find an
    // entry for it in the Region.values() collection, simply return
    if (expected == true) {
      if (value.toString().equals("INVALID") || value.toString().equals("LOCAL_INVALID")) {
        return;
      }
    }

    boolean found = false;
    for (Iterator it=data.values.iterator(); it.hasNext(); ) {
      if (it.next().equals(value)) {
        found = true;
      }
    }
    if (found != expected) {
      throw new TestException("CollectionValidation failure: verifyValues: value found = " + found + " expected = " + expected);
    }
  }

  private void verifyEntries(CollectionInfo data, Object key, Object value, boolean expected) {

    boolean found = false;
    Region.Entry theEntry = null;
    for (Iterator it=data.entries.iterator(); it.hasNext(); ) {
      Region.Entry entry = (Region.Entry)it.next();
      Object entryKey = entry.getKey();
      if (entryKey.equals(key)) {
        found = true;
        theEntry = entry;
        break;
      } 
    }
    if (found != expected) {
      throw new TestException("CollectionValidation failure: verifyEntries: value found = " + found + " expected = " + expected);
    }

    // if it exists, does it have the correct value?
    // Make adjustment for invalidated entries (we won't get "INVALID"
    // from the collection, but null).
    if (value != null) {
      if (value.toString().equals("INVALID") || value.toString().equals("LOCAL_INVALID")) {
        value = null;
      }
    }

    boolean correctValues = false;
    Object entryValue = null;
    if (found) {
      entryValue = theEntry.getValue();
      if (entryValue == null) {
        if (value == null) correctValues = true;
      } else if (entryValue.equals(value)) {
        correctValues = true;
      } 
      if (!correctValues) {
        throw new TestException("CollectionValidation failure: verifyEntries: value found = " + entryValue + " expected = " + value);
      }
    } // if found
  }

  /** Creates a region that was previously destroyed.
   *  
   *  @return An instance of Operation describing the destroy operation.
   */
  public Operation executeCreateRegion(Operation op) {
     String regionName = op.getRegionName();
     Object[] tmp = createRegionWithPath(regionName);
     return op;
  }

  /** Given a region name, return the region instance for it. If the 
   *  region with regionName currently does not exist, create it (and 
   *  any parents required to create it).
   *
   *  @param regionName - The full path name of a region.
   *  
   *  @returns [0] The region specified by regionName
   *           [1] Whether any new regions were created to return [0]
   */
  public Object[] createRegionWithPath(String regionName) {
     boolean regionCreated = false;
     Cache theCache = CacheUtil.getCache();
     StringTokenizer st = new StringTokenizer(regionName, "/", false);
  
     // note: we don't allow destroy of roots of region hierarchy
     String currentRegionName = st.nextToken();
     Region aRegion = theCache.getRegion(currentRegionName); // root region
     RegionAttributes attr = aRegion.getAttributes();
     Region previousRegion = aRegion;
  
     // now that we have the root, see what regions along the region path need to be created
     while (st.hasMoreTokens()) {
        currentRegionName = st.nextToken();
        aRegion = theCache.getRegion(previousRegion.getFullPath() + "/" + currentRegionName);
        if (aRegion == null) {
           try {
              aRegion = previousRegion.createSubregion(currentRegionName, attr);
              Log.getLogWriter().info("Created region " + aRegion.getFullPath());
              regionCreated = true;
           } catch (TimeoutException e) {
              throw new TestException(TestHelper.getStackTrace(e));
           } catch (RegionExistsException e) {
              throw new TestException("Test error; unexpected " + TestHelper.getStackTrace(e));
           }
        }
        previousRegion = aRegion;
     }
     return new Object[] {aRegion, new Boolean(regionCreated)};
  }

  /** Given a CollectionInfo (contains keySet, values, entrySet, etc from region)
   *  verify that the collection cannot be used and that an IllegalStateException
   *  is thrown if attempted.
   *
   *  Enable fine level logging to see IllegalStateExceptions caught.
   */
  private void  verifyCollectionsNotAccessible(CollectionInfo cInfo, Operation op, boolean afterCommit) {

    // accessing regionSets should cause an Exception to be thrown
    boolean exceptionThrown = false;
    boolean beforeCommit = !afterCommit;
    int size; 

    if (!op.isEntryOperation()) {
      return;
    }
 
    attemptOperationsOnSet(cInfo.keys, "keySet");
    attemptOperationsOnSet(cInfo.entries, "entrySet");
    attemptOperationsOnSet(cInfo.values, "values");
  }

  private void verifyIteratorNotAccessible(Iterator iterator, String description) {
    boolean exceptionThrown = false;

    try {
      if (iterator.hasNext()) {
        Object o = iterator.next();
      }
    } catch (IllegalStateException e) {
      exceptionThrown = true;
      Log.getLogWriter().fine("verifyIteratorNotAccessible caught expected Exception " + e + " with " + description + ".hasNext(), continuing test");
    }
    if (!exceptionThrown) {
      throw new TestException("invoking " + description + ".hasNext() did not result in expected IllegalStateException being thrown " + TestHelper.getStackTrace());
    }
  }

  /** Region operations (create-region, destroy-region, etc) are not transactional
   *  and are therefore not subject to the same limitations as other operations.
   *  Ensure that we can access the methods on subRegions for the given CollectionInfo.
   *
   *  Note:  called after commit to verify that we can access txData Collections.
   */
  private void verifySubregionsSetAccessible(CollectionInfo cInfo) {
    try {
      cInfo.subRegions.size();
      cInfo.subRegions.isEmpty();
      cInfo.subRegions.toArray();
      cInfo.subRegions.iterator();
    } catch (Exception e) {
      throw new TestException("access of subregions outside of tx resulted in Exception " + e + " being thrown " + TestHelper.getStackTrace());
    }
    Log.getLogWriter().fine("verifySubregionsSetAccessible successfully executed size(), isEmpty(), toArray() and iterator() on collection");
  }

  /** Try various Set operations (size, iterator, isEmpty and toArray) on the Set
   *  or Collection.
   */
  private void attemptOperationsOnSet(Collection aSet, String description) {
    String[] methods = { "size", "iterator", "isEmpty", "toArray" };
    Class[] params = new Class[0];
    Object[] args = new Object[0];

    Class theClass;
    Method theMethod;

    boolean exceptionThrown;

    for (int i = 0; i < methods.length; i++) {
      exceptionThrown = false;
      try {
        theClass = Class.forName(aSet.getClass().getName());
      } catch (ClassNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }

      try {
        theMethod = theClass.getMethod(methods[i], params);
      } catch (NoSuchMethodException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }

      try {
        theMethod.setAccessible(true);
        Log.getLogWriter().fine("invoking " + theClass.getName() + "." + theMethod.getName() + " on " + description);
        theMethod.invoke(aSet, args);
      } catch (IllegalAccessException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (IllegalArgumentException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (InvocationTargetException e) {
        Throwable targetException = e.getTargetException();
        if (targetException instanceof IllegalStateException) {
          String errStr = targetException.toString();
          if (errStr.indexOf("Region collection") >= 0) {
            Log.getLogWriter().fine("attemptOperationsOnSet caught expected Exception " + targetException + " with " + description + "." + methods[i] + ", continuing test");
            exceptionThrown = true;
          } else {
            throw new TestException(TestHelper.getStackTrace(e));
          }
        } else {
          throw new TestException(TestHelper.getStackTrace(e));
        }
      }
      if (!exceptionThrown) {
        throw new TestException("invoking " + methods[i] + " did not result in expected IllegalStateException being thrown " + TestHelper.getStackTrace());
      }
    }
  }

  /** Call getValueInVM on the given region and key.  For PartitionedRegions,
   *  this calls Region.getEntry and adjusts value returned for Token.INVALID.
   *  For replicated regions, this invokes diskReg.DiskRegUtil.getValueInVM().
   *
   *  @param aRegion - The region to use for the call.
   *  @param key - The key to use for the call.
   *
   *  @returns The value in the VM of key.
   */
  public Object getValueInVM(Region aRegion, Object key) {
     Object value = null;
     // getEntry will not invoke the loader
     Region.Entry entry = aRegion.getEntry(key);
     if (entry != null) {
       value = entry.getValue();
       if (value == null) {   // contains key with null value => Token.INVALID
         value = Token.INVALID;
       }
     }
     return value;
  }
}
