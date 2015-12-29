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

/** 
 * Test the distribution limitations of transactions.  Other tests can be found in
 * <code>MultiVMRegionTestCase</code>.
 * 
 *
 * @author Mitch Thomas
 * @since 4.0
 * @see MultiVMRegionTestCase
 *
 */

package com.gemstone.gemfire.cache30;


import com.gemstone.gemfire.cache.*;

import java.io.File;
import dunit.*;
import hydra.ProcessMgr;

public class TXRestrictionsTest extends CacheTestCase {
  public TXRestrictionsTest(String name) {
    super(name);
  }

  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    return factory.create();
  }

  protected RegionAttributes getDiskRegionAttributes() {
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    File[] diskDirs = new File[1];
    diskDirs[0] = new File("diskRegionDirs/" + ProcessMgr.getProcessId());
    diskDirs[0].mkdirs();
    factory.setDiskStoreName(getCache().createDiskStoreFactory()
                             .setDiskDirs(diskDirs)
                             .setTimeInterval(1000)
                             .setQueueSize(0)
                             .create("TXRestrictionsTest")
                             .getName());
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    return factory.create();
  }

  /** 
   * Check that remote persistent regions cause conflicts
   */
  public void testPersistentRestriction() throws Exception {
    final CacheTransactionManager txMgr = this.getCache().getCacheTransactionManager();
    final String misConfigRegionName = getUniqueName();
    Region misConfigRgn = getCache().createRegion(misConfigRegionName, getDiskRegionAttributes());
    invokeInEveryVM(new SerializableRunnable("testPersistentRestriction: Illegal Region Configuration") {
        public void run() {
          try {
            getCache().createRegion(misConfigRegionName, getDiskRegionAttributes());
            // rgn1.put("misConfigKey", "oldmisConfigVal");
          } catch (CacheException e) {
            fail("While creating region", e);
          }
        }
      });
    misConfigRgn.put("misConfigKey", "oldmisConfigVal");

    txMgr.begin();
    try {
      misConfigRgn.put("misConfigKey", "newmisConfigVal");
      fail("Expected an UnsupportedOperationException with information "
          + "about misconfigured regions");
    } catch (UnsupportedOperationException expected) {
      getSystem().getLogWriter().info("Expected exception: " + expected);
      txMgr.rollback();
    }
    try {
      txMgr.commit();
      // at this point commit should not be possible since it has been rolled
      // back already
      fail("Expected illegal state exception for transaction after rollback");
    } catch (IllegalTransactionStateException expected) {
      getSystem().getLogWriter().info("Expected exception: " + expected);
    }
    misConfigRgn.destroyRegion();
  }
}
