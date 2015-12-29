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
package com.gemstone.gemfire.internal.compression;

import java.util.Properties;

import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

public class CompressionRegionOperationsOffHeapDUnitTest extends
    CompressionRegionOperationsDUnitTest {

  public CompressionRegionOperationsOffHeapDUnitTest(String name) {
    super(name);
  }
  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "1m");
    return props;
  }

  @Override
  protected void createRegion() {
    try {
      SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    
    createCompressedRegionOnVm(getVM(TEST_VM), REGION_NAME, SnappyCompressor.getDefaultInstance(), true);
  }

}
