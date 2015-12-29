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
package resumeTx;

import hydra.Log;
import hydra.blackboard.*;
import tx.*;
import util.*;

import com.gemstone.gemfire.cache.TransactionId;

import java.util.*;

/** ResumeTxBB
 *  A Blackboard for use by resumeable transaction tests.
 *  
 * @author Lynn Hughes-Godfrey
 * @since 6.6
 */
public class ResumeTxBB extends Blackboard {
   
// Blackboard creation variables
static String BB_NAME = "ResumeTx_Blackboard";
static String BB_TYPE = "RMI";

private static ResumeTxBB bbInstance = null;

// SharedMap keys 
public static String NUM_DATASTORES = "NUM_DATASTORES";
public static String OPLIST = "OPLIST";        // aggregate of ops for txId above
public static String ERRSTR = "ERRSTR";
public static String TX_COMPLETED = "TX_COMPLETED";
public static String TX_HOST_DATAPOLICY = "TX_HOST_DATAPOLICY";
public static String TX_ACTIVE = "TX_ACTIVE";  // false if Exceptions caught during ExecuteTx or CommitTx
public static String ACTIVE_TXNS = "ACTIVE_TXNS";  // HashMap of active transactions
// HashMap (with keys = regionName) and values of HashMaps (containing keyValue pairs)
public static String EXPECTED_KEYS_VALUES = "EXPECTED_KEYS_VALUES";
// HashMap (with keys = regionName) and values of Lists (containing destroyed keys)
public static String DESTROYED_ENTRIES = "DESTROYED_ENTRIES";

//sharedCounters
public static int numCompletedTxns;
public static int numResumes;
public static int numCommits;
public static int numFailedCommits;
public static int numSuccessfulCommits;
public static int numRollbacks;
public static int numBegins;
public static int numFailedTries;
public static int numResumesAtCompletion;
public static int valueCounter;
public static int loadController;
public static int resumeOrder;

// Controller Counters for tryResume test
public static int readyToTryResume;
public static int inTryResume;

/**
 *  clear expectedKeysValuesMap (for serial tests only)
 */
public static void clearOpList() {
   getBB().getSharedMap().put(OPLIST, new OpList());
   Log.getLogWriter().fine("ResumeTXBB: OpList cleared");
}

/**
 *  Update BB OpList with the given OpList (for serial tests only)
 */
public static void updateOpList(OpList newList) {
   OpList opList = (OpList)getBB().getSharedMap().get(OPLIST);
   opList.addAll(newList, true);
   getBB().getSharedMap().put(OPLIST, opList);
   Log.getLogWriter().fine("ResumeTXBB: after update, OpList = " + opList);
}

/**
 *  Update expectedKeysValue map based on operation in given opList.
 *  This is a map of map (per region, there is a map of expected keys/values.
 *  Also updates a destroyedEntries Map.  For each region this map contains a list of 
 *  destroyed keys.  (for serial tests on committed transactions only)
 */
public static void computeExpectedKeysValues() {
   OpList opList = (OpList)getBB().getSharedMap().get(OPLIST);

   HashMap expectedKeysValues = new HashMap(); 
   HashMap destroyedEntries = new HashMap();
   // HashMap of region : keyValuePairs (as a secondary map)
   for (int i = 0; i < opList.numOps(); i++) {
      tx.Operation op = opList.getOperation(i);
      String opName = op.getOpName();
      Object regionName = op.getRegionName();
      Object key = op.getKey();
      Object oldValue = op.getOldValue();
      Object newValue = op.getNewValue();
     
      Log.getLogWriter().fine("opList(" + i + ") = " + op.toString());

      if (!expectedKeysValues.containsKey(regionName)) {
        expectedKeysValues.put(regionName, new HashMap());
      }
      Map keyValuePair = (Map)expectedKeysValues.get(regionName);

      if (opName.equals(tx.Operation.ENTRY_DESTROY)) {
         // remove from list of entries to validate key/value
         keyValuePair.remove(key);

         // add to list to verify as destroyed
         if (!destroyedEntries.containsKey(regionName)) {
            destroyedEntries.put(regionName, new ArrayList());
         }
         List keys = (List)destroyedEntries.get(regionName);
         keys.add(key);
         destroyedEntries.put(regionName, keys);
      }  else if (opName.equals(tx.Operation.ENTRY_CREATE) || 
                  opName.equals(tx.Operation.ENTRY_GET_NEW_KEY)) {
         keyValuePair.put(key, newValue);
      } else if (opName.equals(tx.Operation.ENTRY_INVAL)) {
         keyValuePair.put(key, null);
      } else if (opName.equals(tx.Operation.ENTRY_UPDATE) || 
                 opName.equals(tx.Operation.ENTRY_GET_EXIST_KEY)) {
         keyValuePair.put(key, newValue);
      } else {
         Log.getLogWriter().info("Unexpected operation " + op.toString());
         throw new TestException("VerifyTx: Unexpected operation: " + op.toString() + TestHelper.getStackTrace());
      }
      expectedKeysValues.put(regionName, keyValuePair);
   }
   getBB().getSharedMap().put(EXPECTED_KEYS_VALUES, expectedKeysValues);
   getBB().getSharedMap().put(DESTROYED_ENTRIES, destroyedEntries);
   Log.getLogWriter().fine("expectedKeysValue = " + expectedKeysValues.toString());
   Log.getLogWriter().fine("destroyedEntries = " + destroyedEntries.toString());
}

/**
 *  Get the BB
 */
public static ResumeTxBB getBB() {
   if (bbInstance == null) {
      synchronized ( ResumeTxBB.class ) {
         if (bbInstance == null) 
            bbInstance = new ResumeTxBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public ResumeTxBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public ResumeTxBB(String name, String type) {
   super(name, type, ResumeTxBB.class);
}

}
