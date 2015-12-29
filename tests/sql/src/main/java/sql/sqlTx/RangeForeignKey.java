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
package sql.sqlTx;

import hydra.Log;

import java.io.Serializable;
import java.util.HashMap;

//used to determine tx conflict on delete with foreign key constraint 
@SuppressWarnings("serial")
public class RangeForeignKey implements Serializable {
  private boolean wholeRangeKeyAlreadyHeld;
  private HashMap<Integer, Integer> partialRangeKeysHeldByTxIds;
  private int wholeRangeKeyHeldByTxId;
  String key;
  
  public RangeForeignKey(String key) {
    wholeRangeKeyAlreadyHeld = false;
    partialRangeKeysHeldByTxIds = new HashMap<Integer, Integer>();   
    this.key = key;
  }
  
  /**
   * if another txId held the partial range key, delete will fail to lock the foreign key in child
   * @param txId the current txId that operates on the range key, perform delete in parent table
   * @return true when there is conflict on the range key
   *         false when there is no conflict on the range key
   */
  public boolean hasConflictAddWholeRangeKey(int txId) {
    if (wholeRangeKeyAlreadyHeld) {
      if (wholeRangeKeyHeldByTxId == txId) return false; 
      //if the rangeKey has been held by the current txId, then there is no conflict
      else {
        Log.getLogWriter().info("whole range key " + key + " has been held by txId " 
            + wholeRangeKeyHeldByTxId);
        return true;
      }
    }
    else {
      if (partialRangeKeysHeldByTxIds.keySet().size() > 1)  {
        Log.getLogWriter().info("whole range key "  + key + " could not be held");
        return true; //more than one txId current held the partial RangeKey
      } else if (partialRangeKeysHeldByTxIds.keySet().size() == 1 && !partialRangeKeysHeldByTxIds.keySet().contains(txId)){
        Log.getLogWriter().info("whole range key "  + key + "  could not be held, it is held by txId" + 
            partialRangeKeysHeldByTxIds.keySet().iterator().next());
        return true; //only one txId hold the partial range key, but it is not the same as current txId
      } else {
        Log.getLogWriter().info("whole range key "  + key + " will be held by txId " + txId);
        wholeRangeKeyAlreadyHeld = true;
        wholeRangeKeyHeldByTxId = txId;
        return false;
      }   
    }
  }
  
  /**
   * if another txId held the whole range key, then it will fail
   * if another txId held only the partial range key, just add current txId for holding the partial range key
   * @param txId the current txId that operates on the partial range key, perform insert or update in child table
   * @return true when there is conflict on the partial range key
   *         false when there is no conflict on the partial range key
   */
  public boolean hasConflictAddPartialRangeKey(int txId) {
    int count = 1;
    if (wholeRangeKeyAlreadyHeld) {
      if (wholeRangeKeyHeldByTxId == txId) {
        if (partialRangeKeysHeldByTxIds.containsKey(txId))
           count = partialRangeKeysHeldByTxIds.get(txId) + 1;
        partialRangeKeysHeldByTxIds.put(txId, count);
        Log.getLogWriter().info("partial range key " + key + " is being held by this txId " + txId);
        return false; 
      }
      //if the rangeKey has been held by the current txId, then there is no conflict
      else {
        Log.getLogWriter().info("conflict detected as the whole range key " + key + 
            " has been held by another txId " +  wholeRangeKeyHeldByTxId);
        return true;
      }
    }
    else {
      if (partialRangeKeysHeldByTxIds.containsKey(txId))
        count = partialRangeKeysHeldByTxIds.get(txId) + 1;
      partialRangeKeysHeldByTxIds.put(txId, count); //used in child table for foreign key check
      Log.getLogWriter().info("partial range key " + key + " is being held by this txId " + txId);
      return false; 
    }
  }
  
  public void removeWholeRangeKey(int txId) {
    if (wholeRangeKeyAlreadyHeld && wholeRangeKeyHeldByTxId == txId) {
      Log.getLogWriter().info("whole range key "  + key + 
          " will no longer be held by this txId " + txId);
      wholeRangeKeyAlreadyHeld = false;
      wholeRangeKeyHeldByTxId = 0;
    }
  }
  
  public void removePartialRangeKey(int txId) {
    if (partialRangeKeysHeldByTxIds != null) {
      if (partialRangeKeysHeldByTxIds.containsKey(txId)) {
        int count = partialRangeKeysHeldByTxIds.get(txId) - 1; 
        partialRangeKeysHeldByTxIds.put(txId, count);
        Log.getLogWriter().info("removed one partial range key "  + key + 
          " held by this txId " + txId);
      }
    }
  }
  
  public void removeAllPartialRangeKeyByCurTx(int txId) {
    if (partialRangeKeysHeldByTxIds != null) {
      if (partialRangeKeysHeldByTxIds.containsKey(txId)) {
        partialRangeKeysHeldByTxIds.remove(txId); 
        Log.getLogWriter().info("removed all partial range key "  + key + 
          " held by this txId " + txId);
      }
    }
  }
  
  public boolean getWholeRangeKeyAlreadyHeld() {
    return wholeRangeKeyAlreadyHeld;
  }
  
  public int getWholeRangeKeyHeldByTxId() {
    return wholeRangeKeyHeldByTxId;
  }
  
  public String getKey() {
    return key;
  }
}
