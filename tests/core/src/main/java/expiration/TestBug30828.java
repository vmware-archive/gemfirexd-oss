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
//import com.gemstone.gemfire.distributed.DistributedSystem;
import hydra.*;
import java.util.Random;
import util.*;

/**
 * This class contains Hydra tasks that test for bug 30282 (memory
 * leak with expiration and lots of {@linkplain Region#get gets}.
 *
 * @author David Whitlock
 * @since 3.5
 */
public class TestBug30828 {

  /** The region in which work occurs */
  private static Region region;

  /**
   * A Hydra INIT task that creates the region to be operated on.
   * The region uses expiration and a cache loader to populate the
   * region. 
   */
  public static synchronized void createRegion() throws Exception {
    if (region != null) {
      return;
    }

    Cache cache = CacheUtil.createCache();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    factory.setMirrorType(MirrorType.KEYS_VALUES);
    factory.setCacheLoader(new CacheLoader() {
        public Object load(LoaderHelper helper) {
          return String.valueOf(helper.getKey());
        }

        public void close() {

        }
      });
    factory.setStatisticsEnabled(true);
    ExpirationAttributes expire =
      new ExpirationAttributes(300, ExpirationAction.INVALIDATE);
    factory.setEntryTimeToLive(expire);
    region = cache.createVMRegion("TestRegion",
                                  factory.createRegionAttributes());
  }

  /**
   * A Hydra TASK that gets 1000 randomly selected entries from the
   * region. 
   */
  public static void doGets() throws Exception {
    Random random = TestConfig.tab().getRandGen();

    for (int i = 0; i < 1000; i++) {
      if (TestConfig.tab().booleanAt(ExpirPrms.useTransactions, false)) 
         TxHelper.begin();
      region.get(new Integer(random.nextInt(1000)));
      if (TxHelper.exists()) {
         try {
            TxHelper.commit();
         } catch (ConflictException e) {
            // loads for new keys, which turn this load into a create
            // can conflict with other loads that turn this load into
            // a create
         }
      }
    }
  }

}
