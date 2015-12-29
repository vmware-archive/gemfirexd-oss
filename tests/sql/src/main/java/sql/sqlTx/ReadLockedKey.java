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
public class ReadLockedKey implements Serializable {
  private HashMap<Integer, Integer> readLocksHeldByTxIds;
  String key;
  
  public ReadLockedKey(String key) {
    readLocksHeldByTxIds = new HashMap<Integer, Integer>();   
    this.key = key;
  }
  
  public void removeOneReadLockedKeyByCurTx(int txId) {
    if (readLocksHeldByTxIds != null) {
      if (readLocksHeldByTxIds.containsKey(txId)) {
        int count = readLocksHeldByTxIds.get(txId) - 1; 
        readLocksHeldByTxIds.put(txId, count);
        Log.getLogWriter().info("removed one read locked key "  + key + 
          " held by this txId " + txId + " and it still held " + count +
          " read locks for the key");
      }
    }
  }
  
  public void removeAllReadLockedKeyByCurTx(int txId) {
    if (readLocksHeldByTxIds != null) {
      if (readLocksHeldByTxIds.containsKey(txId)) {
        readLocksHeldByTxIds.remove(txId); 
        Log.getLogWriter().info("removed read locked key "  + key + 
          " held by this txId " + txId);
      }
    }
  }
  
  public void addKeyByCurTx(int txId) {
    //count could be used to track one or more resultsets being closed case
    int count = 1;
    if (readLocksHeldByTxIds.containsKey(txId))
      count = readLocksHeldByTxIds.get(txId) + 1;
    readLocksHeldByTxIds.put(txId, count); //used in child table for foreign key check
    Log.getLogWriter().info("read locked key " + key + " is being held by this txId " + txId
        + " for " + count + " times ");
  }
  
  public void findOtherTxId(int txId, ArrayList<Integer> otherTxIds) {
    Set<Integer> txIds = readLocksHeldByTxIds.keySet();
    for (int id: txIds) {
      if (id != txId) {
        //Log.getLogWriter().info("read lock hold by the following txId" + id);
        otherTxIds.add(id);
      }
    }
  }
  
  public void logCurrTxIds() {
    StringBuffer str = new StringBuffer();
    for (int txId: readLocksHeldByTxIds.keySet()) str.append(txId + " ");
    Log.getLogWriter().info("there are " + readLocksHeldByTxIds.size() + " txs -- txId: " +
    		str.toString() + "holding the RR read key " + key );
  }
}
