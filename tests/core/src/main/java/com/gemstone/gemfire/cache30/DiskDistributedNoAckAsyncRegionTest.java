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

package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.*;
import java.io.*;
import hydra.ProcessMgr;

/**
 *
 * @author Eric Zoerner
 *
 */
public class DiskDistributedNoAckAsyncRegionTest extends DiskDistributedNoAckRegionTestCase {
  
  /** Creates a new instance of DiskDistributedNoAckSyncOverflowRegionTest */
  public DiskDistributedNoAckAsyncRegionTest(String name) {
    super(name);
  }
  
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    
    File[] diskDirs = new File[1];
    diskDirs[0] = new File("diskRegionDirs/" + ProcessMgr.getProcessId());
    diskDirs[0].mkdirs();
    factory.setDiskStoreName(getCache().createDiskStoreFactory()
                             .setDiskDirs(diskDirs)
                             .setTimeInterval(1000)
                             .setQueueSize(0)
                             .create("DiskDistributedNoAckAsyncRegionTest")
                             .getName());
    factory.setDiskSynchronous(false);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    return factory.create();
  }
 
}
