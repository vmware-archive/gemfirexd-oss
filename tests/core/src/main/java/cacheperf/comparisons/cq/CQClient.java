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

package cacheperf.comparisons.cq;

import java.util.Random;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.*;
import cacheperf.*;
import cacheperf.comparisons.dataFeed.DataFeedClient;
import distcache.gemfire.GemFireCacheTestImpl;
import hydra.*;
import objects.BatchString;
import objects.BatchStringPrms;

public class CQClient extends DataFeedClient {

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   * INITTASK to register a continuous query.  Requires that the object be of
   * type {@link objects.BatchObject}.  The number of keys matched is {@link
   * objects.BatchStringPrms#batchSize}.  The specific keys are those whose
   * batch prefix equals the client tid modulo the number of batches.  Requires
   * that the batch size evenly divides {@link CachePerfPrms#maxKeys}.
   */
  public static void registerCQsTask() {
    CQClient c = new CQClient();
    c.initialize();
    c.registerCQs();
  }

  private void registerCQs() {
    int numCQs = CQPrms.getNumCQs();
    CqQuery[] cqs = new CqQuery[numCQs];
    for (int i = 0; i < numCQs; i++) {
      cqs[i] = registerCQ(i);
    }
    localcqs.set(cqs);
  }

  private CqQuery registerCQ(int i) {
    String cqName = "cq" + i;
    String query = getQuery(i);
    CqAttributesFactory factory = new CqAttributesFactory();
    factory.addCqListener(CQPrms.getCQListener());
    CqAttributes cqAttrs = factory.create();
    log().info("Registering CQ named \"" + cqName + "\" with query: "
         + query + " and cqAttrs: " + cqAttrs);

    QueryService cqService = CacheHelper.getCache().getQueryService();
    CqQuery cq = null;
    try {
      cq = cqService.newCq(cqName, query, cqAttrs);
    } catch (Exception e) {
      String s = "Failed to register CQ " + cqName;
      throw new CachePerfException(s, e);
    }
    log().info("Successfully registered CQ named " + cqName);

    log().info("Executing CQ " + cq);
    try {
      if (CQPrms.executeWithInitialResults()) {
        SelectResults results = cq.executeWithInitialResults();
      } else {
        cq.execute();
      }
    } catch (Exception e) {
      String s = "Failed to execute CQ " + cqName;
      throw new CachePerfException(s, e);
    }
    log().info("Successfully executed CQ named " + cqName);

    return cq;
  }

  private String getQuery(int i) {
    int strBatchSize = BatchStringPrms.getBatchSize();
    if (this.maxKeys % strBatchSize != 0) {
      String s = BatchStringPrms.nameForKey(BatchStringPrms.batchSize)
               + " does not evenly divide "
               + CachePerfPrms.nameForKey(CachePerfPrms.maxKeys);
      throw new HydraConfigException(s);
    }
    int batches = this.maxKeys/strBatchSize;
    int batchNum = (this.tid + i) % batches;

    String query = "SELECT * "
      + "FROM " + getRegionPath() + " obj "
      + "WHERE obj.batch";
    if (!CQPrms.useMultipleWhereConditionsInCQs()) {
      query = query + " = " + batchNum;
    } else {
      String[] comparators = new String[]{"=", ">", "<", "<=", ">=", "<>"};
      String comparator = comparators[new Random().nextInt(comparators.length)];
      query = query + " " + comparator + " " + batchNum;
    }
    return query;
  }

  private String getRegionPath() {
    if (this.cache instanceof GemFireCacheTestImpl) {
      Region region = ((GemFireCacheTestImpl)this.cache).getRegion();
      return region.getFullPath();
    } else {
      throw new UnsupportedOperationException(this.cache.getClass().getName());
    }
  }

  /**
   * TASK to deregister and reregister previously registered CQs at the rate
   * of {@link CQPrms#throttledOpsPerSec}.  Stats are reported under {@link
   * cacheperf.CachePerfStats#REGISTERINTERESTS}.
   */
  public static void cycleCQsTask() {
    CQClient c = new CQClient();
    c.initialize(REGISTERINTERESTS);
    c.cycleCQs();
  }

  private void cycleCQs() {
    boolean batchDone = false;
    int throttledOpsPerSec = CQPrms.getThrottledOpsPerSec();
    CqQuery[] cqs = (CqQuery[])localcqs.get();
    if (throttledOpsPerSec > 0) {
      do {
        this.timer.reset();
        for (int i = 0; i < throttledOpsPerSec; i++) {
          batchDone = cycleCQ(cqs[i%cqs.length]);
        }
        long remaining = 1000000000 - this.timer.reset();
        if (remaining > 0) {
          MasterController.sleepForMs((int)(remaining/1000000d));
        }
      } while (!batchDone);
    } else {
      int i = 0;
      do {
        batchDone = cycleCQ(cqs[i%cqs.length]);
        ++i;
      } while (!batchDone);
    }
  }

  private boolean cycleCQ(CqQuery cq) {
    int key = getNextKey();
    executeTaskTerminator();
    executeWarmupTerminator();
    deregisterCQ(cq);
    reregisterCQ(cq);
    ++this.batchCount;
    ++this.count;
    ++this.keyCount;
    return executeBatchTerminator();
  }

  private void deregisterCQ(CqQuery cq) {
    try {
      cq.stop();
    } catch (CqException e) {
      String s = "Failed to execute CQ " + cq.getName();
      throw new CachePerfException(s, e);
    }
  }

  private void reregisterCQ(CqQuery cq) {
    try {
      cq.execute();
    } catch (Exception e) {
      String s = "Failed to execute CQ " + cq.getName();
      throw new CachePerfException(s, e);
    }
  }

  private static HydraThreadLocal localcqs = new HydraThreadLocal();
}
