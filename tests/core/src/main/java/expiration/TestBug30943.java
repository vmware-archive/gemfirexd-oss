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
package expiration;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import hydra.*;
import java.util.Random;

/**
 * This contains contains Hydra tasks for testing that bug 30943
 * (distributed system deadlocks when multiple caches are doing
 * expiration on ack or global regions) is fixed.
 *
 * @author David Whitlock
 * @since 3.5
 */
public class TestBug30943 {

  /** The region used by this test */
  private static Region region;

  /**
   * A Hydra INIT task that creates the region to be operated on.  The
   * global region uses expiration and a cache loader to populate the
   * region.
   */
  public static synchronized void createRegion() throws Exception {
    if (region != null) {
      return;
    }

    DistributedSystem system = DistributedConnectionMgr.connect();
    Cache cache = CacheFactory.create(system);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.GLOBAL);
    factory.setCacheLoader(new CacheLoader() {
        public Object load(LoaderHelper helper) {
          return String.valueOf(helper.getKey());
        }

        public void close() {

        }
      });
    factory.setStatisticsEnabled(true);

    // Expire after only 30 seconds so things get a chance to get
    // clogged up
    ExpirationAttributes expire =
      new ExpirationAttributes(30, ExpirationAction.INVALIDATE);
    factory.setEntryTimeToLive(expire);
    region = cache.createVMRegion("TestRegion",
                                  factory.createRegionAttributes());
  }

  /**
   * A Hydra TASK that gets 1000 randomly selected entries from the
   * region.  As the test runs, the region entries will expire
   * potentially causing backup.
   */
  public static void doGets() throws Exception {
    Random random = TestConfig.tab().getRandGen();

    for (int i = 0; i < 1000; i++) {
      region.get(new Integer(random.nextInt(1000)));
    }
  }

}
