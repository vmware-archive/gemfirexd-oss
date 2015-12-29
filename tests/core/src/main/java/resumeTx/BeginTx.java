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

import hydra.*;

import util.*;
import tx.*;

/** BeginTx
 *  A Function to start a new resumeable transaction.
 *  This function also suspends the new transaction and writes the TxId to the ResumeTxBB.
 *  
 * @author Lynn Hughes-Godfrey
 * @since 6.6
 */
public class BeginTx implements Function, Declarable {
  
  public void execute(FunctionContext context) {

    Set filterSet = null;
    DistributedMember dm = CacheHelper.getCache().getDistributedSystem().getDistributedMember();
    if (context instanceof RegionFunctionContext) {
       RegionFunctionContext regionContext = (RegionFunctionContext)context;
       filterSet = regionContext.getFilter();
    }

    Log.getLogWriter().info("executing " + this.getClass().getName() + " in member " + dm + ", invoked from " + context.getArguments().toString() + " with filter " + filterSet);

    TxHelper.begin();
    TransactionId txId = TxHelper.suspend();
    Log.getLogWriter().info("BeginTx returning txId = " + txId);
    context.getResultSender().lastResult(txId);
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
