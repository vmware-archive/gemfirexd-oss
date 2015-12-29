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

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;

/**
 * Tests Partitioned Region with compression.
 * 
 * @author rholmes
 * @since 7.5
 */
public class PartitionedRegionCompressionDUnitTest extends
    PartitionedRegionDUnitTest {
  
  public PartitionedRegionCompressionDUnitTest(String name) {
    super(name);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  protected RegionAttributes getRegionAttributes() {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return super.getRegionAttributes();
    }
    RegionAttributes attrs = super.getRegionAttributes();
    AttributesFactory factory = new AttributesFactory(attrs);
    factory.setCompressor(compressor);
    return factory.create();
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  protected RegionAttributes getRegionAttributes(String type) {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return super.getRegionAttributes(type);
    }
    RegionAttributes ra = super.getRegionAttributes(type);
    AttributesFactory factory = new AttributesFactory(ra);
    if(!ra.getDataPolicy().isEmpty()) {
      factory.setCompressor(compressor);
    }
    return factory.create();
  }
}
