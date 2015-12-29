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

package hydratest.version.replication;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.util.GatewayConflictResolver;
import hydra.HydraConfigException;
import hydra.DiskStoreHelper;

/**
 * Class for testing hydra version support.
 */
public class VersionHelper {

  protected static String getGatewayConflictResolver(Cache c) {
    GatewayConflictResolver gcr = c.getGatewayConflictResolver();
    return (gcr == null) ? null : c.toString();
  }
  protected static String getDiskAttributes(Region r) {
    String dsn = r.getAttributes().getDiskStoreName();
    if (dsn == null) {
      throw new HydraConfigException("Persistence is not configured");
    }
    DiskStore ds = DiskStoreHelper.getDiskStore(dsn);
    return "the DiskStore is" + ds
         + ", the allow force compaction is "
         + ds.getAllowForceCompaction()
         + ", the auto compact is "
         + ds.getAutoCompact()
         + ", the compaction threshold is "
         + ds.getCompactionThreshold()
         + ", the disk dir num is "
         + ds.getDiskDirs().length
         + ", the max oplog size is "
         + ds.getMaxOplogSize()
         + ", the queue size is "
         + ds.getQueueSize()
         + ", the synchronous (from the region) is "
         + r.getAttributes().isDiskSynchronous()
         ;
  }
  protected static String getResourceManager(Cache c) {
    return c.getResourceManager().toString();
  }
  protected static String getMaximum(EvictionAttributes ea) {
    return "undefined";
  }
  protected static String getInterval(EvictionAttributes ea) {
    return "undefined";
  }
  protected static String getObjectSizer(EvictionAttributes ea) {
    Object o = ea.getObjectSizer();
    if (o == null) {
      return "none";
    } else {
      return o.getClass().getName();
    }
  }
}
