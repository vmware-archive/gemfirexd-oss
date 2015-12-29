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

import java.util.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.Token;

import hydra.*;

import util.*;
import tx.*;

/** VerifyTx
 *  A Function to commit a resumeable transaction.
 *  This function is invoked with a TrannsactionId in the argList.  It resumes and commits the 
 *  given transaction id.
 *  
 * @author Lynn Hughes-Godfrey
 * @since 6.6
 */
public class VerifyTx implements Function, Declarable {
  
  public void execute(FunctionContext context) {
    boolean success = true;
    StringBuffer errStr = new StringBuffer();

    DistributedMember dm = CacheHelper.getCache().getDistributedSystem().getDistributedMember();

    // Arguments is an ArrayList with a clientIdString and TxId
    ArrayList argumentList = (ArrayList) context.getArguments();
    String forDM = (String)argumentList.get(0);
    TransactionId txId = (TransactionId)argumentList.get(1);

    Log.getLogWriter().info("executing " + this.getClass().getName() + " in member " + dm + ", invoked from " + forDM + " on txId " + txId);

    // verify destroyed entries
    Map destroyedEntries = (Map)ResumeTxBB.getBB().getSharedMap().get(ResumeTxBB.DESTROYED_ENTRIES);
    Log.getLogWriter().fine("destroyedEntries = " + destroyedEntries);

    for (Iterator it = destroyedEntries.keySet().iterator(); it.hasNext(); ) {
       String regionName = (String)it.next();
       Region aRegion = RegionHelper.getRegion(regionName);
       List keys = (List)destroyedEntries.get(regionName);
       for (int i=0; i < keys.size(); i++) {
          Object key = keys.get(i);
          if (aRegion.containsKey(key)) {
             Log.getLogWriter().info("Expected containsKey to be false, for " + regionName + ":" + key + ", but it was true (key was destroyed in " + txId + ")");
             errStr.append("Expected containsKey to be false, for " + regionName + ":" + key + ", but it was true (key was destroyed in " + txId + ") \n");
          }
       }
    }

    // verify creates, updates, invalidates
    Map expectedKeysValues = (Map)ResumeTxBB.getBB().getSharedMap().get(ResumeTxBB.EXPECTED_KEYS_VALUES);
    Log.getLogWriter().fine("expectedKeysValues = " + expectedKeysValues);

    for (Iterator it = expectedKeysValues.keySet().iterator(); it.hasNext(); ) {
       String regionName = (String)it.next();
       Region aRegion = RegionHelper.getRegion(regionName);
       Map keyValuePair = (Map)expectedKeysValues.get(regionName);
       for (Iterator kvi = keyValuePair.keySet().iterator(); kvi.hasNext(); ) {
          Object key = kvi.next();
          Object value = keyValuePair.get(key);
          
          //verify against region data
          if (!aRegion.containsKey(key)) {
             Log.getLogWriter().info("Expected containsKey() to be true for " + regionName + ":" + key + ", but it was false.  txId = " + txId);
             errStr.append("Expected containsKey() to be true for " + regionName + ":" + key + ", but it was false, txId = " + txId + "\n");
          }
          if (value == null && aRegion.containsValueForKey(key)) {
             Log.getLogWriter().info("Expected containsValueForKey to be false, for " + regionName + ":" + key + ", but it was true, txId = " + txId);
             errStr.append("Expected containsValueForKey to be false, for " + regionName + ":" + key + ", but it was true, txId = " + txId + "\n");
          }

          if (value != null && !aRegion.containsValueForKey(key)) {
             Log.getLogWriter().info("Expected containsValueForKey() to be true for " + regionName + ":" + key + ", but it was false, txId = " + txId);
             errStr.append("Expected containsValueForKey() to be true for " + regionName + ":" + key + ", but it was false, txId = " + txId + "\n");
          }

          Object actualValue = null;
          // Avoid doing a load 
          Region.Entry entry = aRegion.getEntry(key);
          if (entry != null) {
             actualValue = entry.getValue();
          }

          // test really works off ValueHolder.modVal ...
          if (actualValue instanceof BaseValueHolder) {
            actualValue = ((BaseValueHolder)actualValue).modVal;
            Log.getLogWriter().fine("actualValue for " + regionName + ":" + key + " is an Integer " + actualValue);
            if (!actualValue.equals(value)) {
               Log.getLogWriter().info("Expected value for " + regionName + ":" + key + " to be " + value + ", but found " + actualValue + " txId = " + txId);
               errStr.append("Expected value for " + regionName + ":" + key + " to be " + value + ", but found " + actualValue + ", txId = " + txId + "\n");
            }
          } else {
            Log.getLogWriter().info("WARNING: actual value retrieved from cache is not a ValueHolder, is " + actualValue);
          }
       }
    }

    if (errStr.length() > 0) { 
       success = false;
       ResumeTxBB.getBB().getSharedMap().put(ResumeTxBB.ERRSTR, errStr.toString());
    }
    Log.getLogWriter().info("VerifyTx returning " + success);
    context.getResultSender().lastResult(new Boolean(success));
  }

  public String getId() {
    return this.getClass().getName();
  }

  public boolean hasResult() {
    return true;
  }

  public boolean optimizeForWrite() {
    return true;
  }

  public void init(Properties props) {
  }

  public boolean isHA() {
    return true;
  }
}
