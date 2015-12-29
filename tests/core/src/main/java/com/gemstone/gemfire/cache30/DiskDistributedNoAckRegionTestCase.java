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

/**
 *
 * @author Eric Zoerner
 *
 */
public abstract class DiskDistributedNoAckRegionTestCase extends DistributedNoAckRegionTest {
  final protected DiskRegionTestImpl regionTestImpl;
  
  /** Creates a new instance of DiskDistributedNoAckRegionTest */
  public DiskDistributedNoAckRegionTestCase(String name) {
    super(name);
    regionTestImpl = new DiskRegionTestImpl(this);
  }
  
  public void testCreateDiskRegion() throws CacheException {
    this.regionTestImpl.testCreateDiskRegion();
  }
  
  public void testBackupFillInValues() throws CacheException {
        this.regionTestImpl.testBackupFillValues();
  }
}
