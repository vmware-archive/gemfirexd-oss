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
package cacheperf.comparisons.newWan;

import com.gemstone.gemfire.internal.NanoTimer;

import hydra.MasterController;
import cacheperf.CachePerfClient;

public class NewWanPerfClient extends CachePerfClient {
  
  /**
   * TASK to put objects with new wan and wait for the sender queues to drain.
   */
  public static void putDataGWSenderTask() {
    NewWanPerfClient c = new NewWanPerfClient();
    c.initialize(PUTS);
    c.putDataGWSender();
  }

  private void putDataGWSender() {
    if (this.useTransactions) {
      this.begin();
    }
    //compute number of iterations before every pause of 100 sec,
    //so that required input throughput is achieved.
    int sleepMs = 100;   
    int itrs = NewWanHelper.inputPutsPerSecPerThread / (1000 / sleepMs) ; 
    boolean batchDone = false;
    do {
      executeTaskTerminator(); // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      long begin = System.currentTimeMillis();
      for (int i = 0; i < itrs; i++) {
        int key = getNextKey();
        put(key);
        ++this.batchCount;
        ++this.count;
        ++this.keyCount;
        ++this.iterationsSinceTxEnd;        
      } 
      long diff = System.currentTimeMillis() - begin;
      if(sleepMs > diff){
        MasterController.sleepForMs(sleepMs - (int)diff);
      }
      batchDone = executeBatchTerminator(); // commits at batch termination
    } while (!batchDone);
  }
}
