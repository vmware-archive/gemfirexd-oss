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

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

/**
 * Tests Distributed Ack Overflow Region with ConcurrencyChecksEnabled and OffHeap memory.
 * 
 * @author Kirk Lund
 * @since 7.5
 */
@SuppressWarnings({ "deprecation", "serial" })
public class DistributedAckOverflowRegionCCEOffHeapDUnitTest extends DistributedAckOverflowRegionCCEDUnitTest {

  public DistributedAckOverflowRegionCCEOffHeapDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "10m");
    return props;
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  protected RegionAttributes getRegionAttributes() {
    RegionAttributes attrs = super.getRegionAttributes();
    AttributesFactory factory = new AttributesFactory(attrs);
    factory.setEnableOffHeapMemory(true);
    return factory.create();
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  protected RegionAttributes getRegionAttributes(String type) {
    RegionAttributes ra = super.getRegionAttributes(type);
    AttributesFactory factory = new AttributesFactory(ra);
    if(!ra.getDataPolicy().isEmpty()) {
      factory.setEnableOffHeapMemory(true);
    }
    return factory.create();
  }
}
