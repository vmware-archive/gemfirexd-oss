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

package cacheperf.comparisons.parReg;

import cacheperf.CachePerfClient;
import cacheperf.CachePerfPrms;
import com.gemstone.gemfire.cache.Region;
import distcache.gemfire.GemFireCachePrms;
import hydra.ConfigPrms;
import hydra.EdgeHelper;
import hydra.RegionHelper;
import objects.ObjectHelper;

/**
 * Client that can handle multiple regions.
 */
public class MultiRegionClient extends CachePerfClient {

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   * TASK to create the region named {@link
   * distcache.gemfire.GemFireCachePrms#regionName}.
   */
  public static synchronized void createRegionTask() {
    MultiRegionClient c = new MultiRegionClient();
    c.initHydraThreadLocals();
    c.createRegion();
    c.updateHydraThreadLocals();
  }
  private void createRegion() {
    String regionName = GemFireCachePrms.getRegionName();
    String regionConfig = ConfigPrms.getRegionConfig();
    if (RegionHelper.getRegion(regionName) == null) {
      RegionHelper.createRegion(regionName, regionConfig);
    }
    Region region = RegionHelper.getRegion(regionName);
    EdgeHelper.addThreadLocalConnection(region);
  }

  /**
   * TASK to create objects of type {@link cacheperf.CachePerfPrms#objectType}
   * in the region named {@link distcache.gemfire.GemFireCachePrms#regionName}.
   */
  public static void createDataInRegionTask() {
    MultiRegionClient c = new MultiRegionClient();
    c.initialize(CREATES);
    c.createDataInRegion();
  }
  private void createDataInRegion() {
    String regionName = GemFireCachePrms.getRegionName();
    Region region = RegionHelper.getRegion(regionName);
    if (this.useTransactions) {
      this.begin();
    }
    do {
      int key = getNextKey();
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      createInRegion(key, region);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator()); // commits at batch termination
  }
  private void createInRegion(int i, Region region) {
    Object key = ObjectHelper.createName(this.keyType, i);
    String objectType = CachePerfPrms.getObjectType();
    Object val = ObjectHelper.createObject(objectType, i);
    long start = this.statistics.startCreate();
    region.create(key, val);
    this.statistics.endCreate(start, this.isMainWorkload, this.histogram);
  }
}
