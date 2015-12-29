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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

@SuppressWarnings("serial")
public class ForeignKeyLocked implements Serializable {
  //could be used for RC tracking foreign keys during insert
  //could be used for tx-batching enabled testing
  private HashMap<Integer, Integer> keysHeldByTxIds;
  String key;
  
  public ForeignKeyLocked(String key) {
    keysHeldByTxIds = new HashMap<Integer, Integer>();   
    this.key = key;
  }
  
  public void removeOneLockedKeyByCurTx(int txId) {
    if (keysHeldByTxIds != null) {
      if (keysHeldByTxIds.containsKey(txId)) {
        int count = keysHeldByTxIds.get(txId) - 1; 
        keysHeldByTxIds.put(txId, count);
        Log.getLogWriter().info("removed one locked key "  + key + 
          " held by this txId " + txId + " and it still held " + count +
          " locks for the key");
      }
    }
  }
  
  public HashMap<Integer, Integer> getKeysHeldByTxIds() {
    return this.keysHeldByTxIds;
  }
  
  public void removeAllLockedKeyByCurTx(int txId) {
    if (keysHeldByTxIds != null) {
      if (keysHeldByTxIds.containsKey(txId)) {
        keysHeldByTxIds.remove(txId); 
        Log.getLogWriter().info("removed locked key "  + key + 
          " held by this txId " + txId);
      }
    }
  }
  
  public void addKeyByCurTx(int txId) {
    //count could be used to track one or more resultsets being closed case
    int count = 1;
    if (keysHeldByTxIds.containsKey(txId))
      count = keysHeldByTxIds.get(txId) + 1;
    keysHeldByTxIds.put(txId, count); //used in child table for foreign key check
    Log.getLogWriter().info("locked key " + key + " is being held by this txId " + txId
        + " for " + count + " times ");
  }
  
  public void findOtherTxId(int txId, ArrayList<Integer> otherTxIds) {
    Set<Integer> txIds = keysHeldByTxIds.keySet();
    for (int id: txIds) {
      if (id != txId) {
        //Log.getLogWriter().info("lock hold by the following txId" + id);
        otherTxIds.add(id);
      }
    }
  }
  
  public boolean detectOtherTxIdConflict(int txId) {
    Set<Integer> txIds = keysHeldByTxIds.keySet();
    for (int id: txIds) {
      if (id != txId) {
        Log.getLogWriter().info("lock hold by the following txId" + id);
        return true;
      }
    }
    return false;
  }
  
  public boolean detectOtherTxIdConflict(int txId, StringBuilder str) {
    Set<Integer> txIds = keysHeldByTxIds.keySet();
    for (int id: txIds) {
      if (id != txId) {
        str.append("The lock has been held by other txId: " + id + " for key " + key);
        Log.getLogWriter().info("lock hold by the following txId" + id);
        return true;
      }
    }
    return false;
  }
  
  public void logCurrTxIds() {
    StringBuffer str = new StringBuffer();
    for (int txId: keysHeldByTxIds.keySet()) str.append(txId + " ");
    Log.getLogWriter().info("there are " + keysHeldByTxIds.size() + " txs -- txId: " +
    		str.toString() + "holding the key " + key );
  }
}
