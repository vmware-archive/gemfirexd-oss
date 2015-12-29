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
import hydra.*;
import com.gemstone.gemfire.cache.*;

/**
 * A class that contains the operations that have occurred since a certain
 * point in time (maybe in a transactin, maybe not)
 */
public class OpList implements java.io.Serializable {

private List opList = new ArrayList();

/** Set the operation at the given index to the given operation.
 *
 *  @param index The index to set.
 *  @param Op The Operation to put at index.
 *
 *  @returns The previous value at index.
 */
public Object set(int index, Operation op) {
   Object returnObj = opList.get(index);
   opList.set(index, op);
   return returnObj;
}
   
/** Add all operations in given opList to the receiver.
 *
 *  @param toBeAdded The opList to add to the receiver.
 */
public void addAll(OpList toBeAdded) {
   if (toBeAdded == null)
      return;
   for (int i = 0; i < toBeAdded.numOps(); i++) {
      this.add(toBeAdded.getOperation(i));
   }
}
   
/** Add an operation to this instance.
 *
 *  @param anOp The operation to add.
 */
public void add(Operation anOp) {
   if (!(opList.contains(anOp)))
      opList.add(anOp);
}

/** Add all operations in the given opList to the receiver (allow duplicates)
 *
 * @param toBeAdded The opList to add
 * @param allowDuplicates allow duplicates in the resulting opList
 */
public void addAll(OpList toBeAdded, boolean allowDuplicates) {
   if (toBeAdded == null)
      return;
   for (int i = 0; i < toBeAdded.numOps(); i++) {
      if (allowDuplicates) {
         opList.add(toBeAdded.getOperation(i));
      } else {
         this.add(toBeAdded.getOperation(i));
      }
   }
}

/** Remove operation at index provided from this instance.
 *
 *  @param index - the index of the operation to remove
 */
public void remove(int index) {
      opList.remove(index);
}

/** 
 * Support contains on opList
 *
 * @param anOp - the operation to test 
 */
public boolean contains(Operation anOp) {
  return opList.contains(anOp);
}
   
/** Return the number of operations in this instance.
 *
 *  @returns The number of operations contained in this instance.
 */
public int numOps() {
   return opList.size();
}

/** Return the operation at the given index.
 *
 *  @param The index of the Operation to return.
 *
 *  @returns The Operation at index i.
 */
public Operation getOperation(int i) {
   return (Operation)(opList.get(i));
}

/** Return a subset of the receiver containing operations which
 *  match the given opName.
 *
 *  @param opName The name of the operation to match.
 *
 *  @returns An OpList of opName Operations.
 */
public OpList getOps(String opName) {
   OpList resultList = new OpList();
   for (int i = 0; i < opList.size(); i++) {
      Operation op = (Operation)opList.get(i);
      if (op.getOpName().equals(opName))
         resultList.add(op);
   }
   return resultList;
}

/** Return a subset of the receiver which contains any operations
 *  that occurred on the given region.
 *
 *  @param regionName The name of the region.
 *
 *  @returns An OpList of Operations that occurred on the region
 */
public OpList getOpsForRegion(String regionName) {
   OpList resultList = new OpList();
   for (int i = 0; i < opList.size(); i++) {
      Operation op = (Operation)opList.get(i);
      if (op.getRegionName().equals(regionName))
         resultList.add(op);
   }
   return resultList;
}

/** Return a subset of the receiver which contains any operations
 *  that occurred on the given key.
 *
 *  @param key The key of interest for Operations.
 *
 *  @returns An OpList of Operations that occurred using the given key.
 */
public OpList getOpsForKey(String key) {
   OpList resultList = new OpList();
   for (int i = 0; i < opList.size(); i++) {
      Operation op = (Operation)opList.get(i);
      if (op.getKey().equals(key))
         resultList.add(op);
   }
   return resultList;
}

/** Return a subset of the receiver which contains any operations
 *  that occurred on the given key in the given region.
 *
 *  @param regionName The name of the region.
 *  @param key The key of interest for Operations.
 *
 *  @returns An OpList of Operations that occurred on the region with
 *           the given key
 */
public OpList getOpsForRegionAndKey(String regionName, Object key) {
   OpList resultList = new OpList();
   for (int i = 0; i < opList.size(); i++) {
      Operation op = (Operation)opList.get(i);
      boolean regionMatches = (regionName == null) ? (op.getRegionName() == null) : 
                                                     (regionName.equals(op.getRegionName()));
      boolean keyMatches = (key == null) ? (op.getKey() == null) : 
                                           (key.equals(op.getKey()));
      if (regionMatches && keyMatches) 
         resultList.add(op);
   }
   return resultList;
}

/** Return a subset of the receiver which contains any operations
 *  that were affected by the given operation.
 *
 *  @param operation The operation to use to determine which ops in
 *                   the opList were affected.
 *
 *  @returns An OpList of Operations that are affected by the operation.
 */
public OpList getOpsAffectedBy(Operation operation) {
   OpList resultList = new OpList();
   for (int i = 0; i < opList.size(); i++) {
      Operation op = (Operation)opList.get(i);
      if (op.affectedBy(operation))
         resultList.add(op);
   }
   return resultList;
}

/** Returns true if the OpList contains an operation with opName,
 *  returns false otherwise.
 *  
 *  @param opName The name of the operation to test for.
 *
 *  @returns True if the OpList contains an operation with the same
 *           name as opName, false otherwise.
 */
public boolean containsOpName(String opName) {
   for (int i = 0; i < opList.size(); i++) {
      Operation op = (Operation)opList.get(i);
      if (op.getOpName().equals(opName)) {
         return true;
      }
   }
   return false;
}

public OpList collapse() {
   return collapse(TxBB.TXACTION_NONE);
}

/** 
 *  Collapse the opList for the specific conditions.  txAction is
 *  "none" by default, but can also be set to "commit" or "rollback"
 *  for special handling.
 */
public OpList collapse(String txAction) {

  // get separate lists for entry & region ops
  OpList entryOps = getEntryOps();
  OpList regionOps = getRegionOps();

  // For entry operations, only need LAST operation 
  // applied to the exact same entry in the exact same region
  // we need to keep the true original value though 
  // even across multiple operations on the same region/key
  entryOps = getEntriesWithUniqueKeys(entryOps, txAction);

  // filter out entry ops on destroyed regions
  // these will cause write conflicts at commit time, but we don't 
  // want to attempt to validate these (since we can't get 'actualValues')
  entryOps = getEntriesNotAffectedByRegionDestroy(regionOps, entryOps);
  
  // filter out invalidate entry ops on invalidated regions
  // The region-invalidate is immediate ... the entry invalidate
  // can't really be verified (because the old values don't
  // indicate what they were when the transaction was started).
  // (we expect { true, true, oldValue } but will see { true, false, INVALID }.
  entryOps = getEntriesNotAffectedByRegionInvalidates(regionOps, entryOps);

  // filter out region ops which are overridden by other region ops
  regionOps = getValidRegionOps(regionOps);

  // Since region operations are applied immediately (not at commit time)
  // some entry operations (performed before OR after the region operation)
  // on related regions will make it impossible to validate the associated 
  // region operation (e.g. regionInvalidate w/ entry-create)
  // drop unverifiable region operations from the list
  // NOTE1:  RegionDestroys are handled differently, the region operation
  // IS verifiable, but the affected entry operations are not; these 
  // entry ops are filtered out in the preceeding code block (getValidRegionOps)
  // NOTE2:  RegionInvalidates can't be verified if a create or update
  // occurs ... entryInval or entryDestroy doesn't affect ability to 
  // verify.
  // IMPORTANT NOTE:  This requires the ORIGINAL list of entryOps!
  regionOps = getVerifiableRegionOps(regionOps, this.getEntryOps());

  // combine remaining entry & regionOps for our 'collapsed' list of ops
  OpList collapsedOps = new OpList();  
  collapsedOps.addAll(entryOps);
  collapsedOps.addAll(regionOps);

  Log.getLogWriter().info("COLLAPSED OPS: " + collapsedOps.toString());
  return collapsedOps;
}

/** a slight variation of the collapse() for TxListener event handling */
public OpList collapseForEvents(String txAction) {

  // get separate lists for entry & region ops
  OpList entryOps = getEntryOps();
  OpList regionOps = getRegionOps();

  // For entry operations, only need LAST operation
  // applied to the exact same entry in the exact same region
  // we need to keep the true original value though
  // even across multiple operations on the same region/key
  entryOps = getEntriesWithUniqueKeys(entryOps, txAction);

  // for remote VMs, filter out entry ops on destroyed regions
  // since these won't be recreated within same Tx
  if (!TxUtil.inTxVm()) {
     entryOps = getEntriesNotAffectedByRegionDestroy(regionOps, entryOps);
  }
  
  // combine remaining entry & regionOps for our 'collapsed' list of ops
  OpList collapsedOps = new OpList();  
  collapsedOps.addAll(entryOps);

  Log.getLogWriter().info("COLLAPSED EVENT OPS: " + collapsedOps.toString());
  return collapsedOps;
}

/** Return an OpList containing only entry operations in the receiver.
 *
 */
public OpList getEntryOps() {

  OpList entryOps = new OpList();

  for (int i=0; i < this.numOps(); i++) {
    Operation op = this.getOperation(i);
    if (op.isEntryOperation()) {
      entryOps.add(op);
    }
  }

  Log.getLogWriter().fine("COLLAPSE: entryOps = " + entryOps.toString());
  return entryOps;
}

/** Return an OpList containing only region operations in the receiver
 */
private OpList getRegionOps() {

  OpList regionOps = new OpList();

  for (int i=0; i < this.numOps(); i++) {
    Operation op = this.getOperation(i);
    if (op.isRegionOperation()) {
      regionOps.add(op);
    }
  }

  Log.getLogWriter().fine("COLLAPSE: regionOps = " + regionOps.toString());
  return regionOps;
}

private boolean updatedInvalidatedEntry(Operation firstOp, Operation secondOp) {
   boolean rc = false;
   if (firstOp.isEntryInvalidate()) {
     if (secondOp.isEntryGet() || secondOp.isEntryUpdate()) {
       rc = true;
     }
   }
   return rc;
}

private boolean invalidatedExistingEntry(Operation firstOp, Operation secondOp) {
   boolean rc = false;
   if (secondOp.isEntryInvalidate()) {
     if (firstOp.getOldValue() != null) {
       rc = true;
     }
   }
   return rc;
}

public OpList handleDoubleInvalidates(OpList entryOps) {

  OpList newList = new OpList();

  for (int i=0; i < entryOps.numOps(); i++) {
    Operation op = entryOps.getOperation(i);
    boolean intermediateOp = false;   // set to true if we see a non-invalidate op between the invalidates
    for (int j=i+1; j < entryOps.numOps(); j++) {
      Operation tmpOp = entryOps.getOperation(j);
      if (op.entryOpAffectedBy(tmpOp) && op.isEntryInvalidate()) {

        // double invalidates should keep initial invalidate only
        // remove subsequent invalidates
        if (tmpOp.isEntryInvalidate()) {
          if (!intermediateOp) {
            entryOps.remove(j);
            j--;
          }
        } else {
          intermediateOp = true;
        }
      }
    }
    // write initial op to list (subsequent invalidates for this entry have now been removed from the original entryOps list)
    newList.add(op);
  }

  Log.getLogWriter().fine("handleDoubleInvalidates = " + newList.toString());
  return newList;
}

public OpList removeNoOpGets(OpList entryOps) {

  OpList newList = new OpList();

  for (int i=0; i < entryOps.numOps(); i++) {
    Operation op = entryOps.getOperation(i);
    // skip over any no-ops (like a get where oldValue == newValue)
    if (op.isGetOperation()) {
      Object oldVal = op.getOldValue();
      Object newVal = op.getNewValue();
      if ((oldVal != null) && (newVal != null)) {
        if (oldVal.equals(newVal)) {
          continue;
        }
      }
    }
    // write initial op to list (subsequent invalidates for this entry have now been removed from the original entryOps list)
    newList.add(op);
  }

  Log.getLogWriter().fine("removeNoOpGets = " + newList.toString());
  return newList;
}


// Note: entry operations collapse needs to take into account
// the fact that if inTxThread() && TransactionInProgress ...
// if two operations have matching keys, we need to look at the
// operations themselves (whoever is last doesn't necessarily win).
// Consider the case where we do an invalidate followed by a get
// (which does a load). The loaded value only goes to committed state
// so the transactional thread will NOT see if while the tx is in progress.
public OpList getEntriesWithUniqueKeys(OpList entryOps) {
   return getEntriesWithUniqueKeys(entryOps, TxBB.TXACTION_NONE); 
}

// Allows the invocations from TxListeners to know about txAction (Commit vs. Rollback)
public OpList getEntriesWithUniqueKeys(OpList entryOps, String txAction) {

  // filtering (see BUG 41603) 
  // remove get operations (oldVal = newVal) since these are no-ops
  entryOps = removeNoOpGets(entryOps);

  // With double invalidates, the 2nd invalidate is a no-op, so 
  // unlike other operations, we need to keep the first (invalidate) operation
  // Filter out the "no-ops" before processing other operation collapsing.
  entryOps = handleDoubleInvalidates(entryOps);

  // Fix for Test bug 39769
  // For tx view tests, we need to know if dataPolicy for the TxVm is EMPTY
  boolean isTxVmProxy = false;
  String txClientName = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_VM_CLIENTNAME);
  if (txClientName != null) {
     String mapKey = TxBB.DataPolicyPrefix + txClientName;
     String dataPolicy = (String)(TxBB.getBB().getSharedMap().get(mapKey));
     if (dataPolicy != null && dataPolicy.equals("empty")) {
        isTxVmProxy = true;
     }
  }
  Log.getLogWriter().info("getEntriesWithUniqueKeys: txAction = " + txAction + " isTxVmProxy = " + isTxVmProxy);

  OpList newList = new OpList();

  for (int i=0; i < entryOps.numOps(); i++) {
    Operation op = entryOps.getOperation(i);
    for (int j=i+1; j < entryOps.numOps(); j++) {
      Operation tmpOp = entryOps.getOperation(j);
      if (op.entryOpAffectedBy(tmpOp)) {
        boolean keepOp = false;
        boolean keepTmpOp = true;

        // We have to collapse create + otherOp differently,
        // we need to keep the create, but with the final value
        // for this entry.  

        // handle special case of create/destroy (since this = no-op)
        // this includes get's which do a load!
        if (op.isEntryCreate() || (op.isEntryGet() && op.getOldValue()==null)) {
          // special handling if tmpOp == entryDestroy (becomes a no-op)
          // we don't want either operation (not verifiable)
          if (tmpOp.isEntryDestroy()) {
             if (TxUtil.inTxVm() && isTxVmProxy) {
                // keep tmpOp (the destroy) in the txVM if DataPolicy.EMPTY
             } else {
                keepTmpOp = false;
             }
          } 
        } 

        if (!keepTmpOp) {
           entryOps.remove(j);
           j--;
        }

        // if we're collapsing op + tmpOp => tmpOp make any necessary adjustments to 
        // opName and oldValue
        if (!keepOp) {
          if (keepTmpOp) {
            // restrict cases where we alter the tmpOp.opName
            if (!tmpOp.isEntryDestroy() && 
                !updatedInvalidatedEntry(op, tmpOp) &&
                !invalidatedExistingEntry(op, tmpOp) &&
                !op.isEntryDestroy()) {
              tmpOp.setOpName(op.getOpName());
            }
            tmpOp.setOldValue(op.getOldValue());
            entryOps.set(j, tmpOp);
          }
          op = null;
          break;
        }  
      }
    }
    // Add final op affecting this region/key into new list
    if (op != null) {
      newList.add(op);
    }
  }

  Log.getLogWriter().fine("ENTRY OPS WITH UNIQUE KEYS = " + newList.toString());
  return newList;
}

private OpList getVerifiableRegionOps(OpList regionOps, OpList entryOps) {
 
  OpList newList = new OpList();

  for (int i=0; i < regionOps.numOps(); i++) {
    Operation regionOp = regionOps.getOperation(i);

    // Any regionDestroy is verifiable -- the related entry ops are not
    if (regionOp.isRegionDestroy()) {
      newList.add(regionOp);
      continue;          // go to next region op in list
    }

    boolean verifiable = true;
    for (int j=0; j < entryOps.numOps(); j++) {
      Operation entryOp = entryOps.getOperation(j);
      Log.getLogWriter().fine("(ValidRegionOps) RegionOp = " + regionOp.toString() + " Looking at entryOp = " + entryOp.toString());
      if (entryOp.getRegionName().startsWith(regionOp.getRegionName())) {
        Log.getLogWriter().fine("(ValidRegionOps) entryOp affected by RegionOp (based on regionName)");
        if (regionOp.isRegionInvalidate() && entryOp.isEntryInvalidate()) {
          Log.getLogWriter().fine("(ValidRegionOps) verifiable = true");
          verifiable = true;               
        } else {
          Log.getLogWriter().fine("(ValidRegionOps) verifiable = false");
          verifiable = false;
          break;
        }
      }
    }
    if (verifiable) {
      Log.getLogWriter().fine("(VaildRegionOps) adding regionOp " + regionOp.toString() + " to list of verifiable region operations");
      newList.add(regionOp);
    }
  }
  Log.getLogWriter().fine("VERIFIABLE REGION OPS " + newList.toString());
  return newList;
}

private OpList getEntriesNotAffectedByRegionDestroy(OpList regionOps, OpList entryOps) {
  
  for (int i=0; i < regionOps.numOps(); i++) {
    Operation regionOp = regionOps.getOperation(i);
   
    if (!regionOp.isRegionDestroy()) {
      Log.getLogWriter().finer("(AffectedByRegionDestroy) Ignoring RegionOp = " + regionOp.toString());
      continue;             // no change, move to next region op in list
    }

    Log.getLogWriter().finer("(AffectedByRegionDestroy) RegionDestroyOp = " + regionOp.toString());
    for (int j=0; j < entryOps.numOps(); j++) {
      Operation entryOp = entryOps.getOperation(j);
      Log.getLogWriter().finer("(AffectedByRegionDestroy) Looking at entryOp = " + entryOp.toString());
      if (entryOp.getRegionName().startsWith(regionOp.getRegionName())) {
        Log.getLogWriter().finer("(AffectedByRegionDestroy) Removing entryOp = " + entryOp.toString());
        entryOps.remove(j);
        j--;                        // backup so we get the next op!
      }
    }
  }
  Log.getLogWriter().fine("ENTRY OPS NOT AFFECTED BY REGION DESTROY " + entryOps.toString());
  return entryOps;
}

private OpList getEntriesNotAffectedByRegionInvalidates(OpList regionOps, OpList entryOps) {

  for (int i=0; i < regionOps.numOps(); i++) {
    Operation regionOp = regionOps.getOperation(i);
   
    if (!regionOp.isRegionInvalidate()) {
      Log.getLogWriter().finer("(AffectedByRegionInval) Ignoring regionOp = " + regionOp.toString());
      continue;             // no change, move to next region op in list
    }

    Log.getLogWriter().finer("(AffectedByRegionInval) RegionOp = " + regionOp.toString());
    for (int j=0; j < entryOps.numOps(); j++) {
      Operation entryOp = entryOps.getOperation(j);
      Log.getLogWriter().finer("(AffectedByRegionInval) Looking at entryOp = " + entryOp.toString());

      // If we find an entry op in the region hierarchy under the 
      // invalidate -- we have to remove it from our list
      // as we won't be able to validate it against the original values
      if (entryOp.getRegionName().startsWith(regionOp.getRegionName())) {
        Log.getLogWriter().finer("(AffectedByRegionInval) Removing entryOp = " + entryOp.toString());
        entryOps.remove(j);
        j--;                      // backup to get the next op!
      }
    }
  }
  Log.getLogWriter().fine("ENTRY OPS NOT AFFECTED BY REGION INVALIDATE " + entryOps.toString());
  return entryOps;
}

// Get RegionOps not overridden by other regionOps
private OpList getValidRegionOps(OpList regionOps) {

  OpList newList = new OpList();

  for (int i=0; i < regionOps.numOps(); i++) {
    Operation op = regionOps.getOperation(i);
    for (int j=0; j < regionOps.numOps(); j++) {
      // skip comparison with same op
      if (i == j) {
        continue;
      }

      Operation tmpOp = regionOps.getOperation(j);
      if (op.getRegionName().startsWith(tmpOp.getRegionName())) {
        op = null;
        break;
      }
    }
    if (op != null) {
      newList.add(op);
    }
  }
  return newList;
}

/** Return a string representation of the receiver */
public String toString() {
   StringBuffer aStr = new StringBuffer();
   if (opList.size() == 0)
      aStr.append("OpList is empty");
   else
      aStr.append("OpList containing " + opList.size() + " operations:");
   for (int i = 0; i < opList.size(); i++) {
      aStr.append("\n" + opList.get(i).toString());
   }
   return aStr.toString();
}

}
