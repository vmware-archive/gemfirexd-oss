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
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;

public class DistributedAckOverflowRegionCCEDUnitTest extends
    DistributedAckRegionCCEDUnitTest {

  public DistributedAckOverflowRegionCCEDUnitTest(String name) {
    super(name);
  }

  @Override
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(true);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
        5, EvictionAction.OVERFLOW_TO_DISK));
    return factory.create();
  }
  
  @Override
  protected RegionAttributes getRegionAttributes(String type) {
    RegionAttributes ra = getCache().getRegionAttributes(type);
    if (ra == null) {
      throw new IllegalStateException("The region shortcut " + type
                                      + " has been removed.");
    }
    AttributesFactory factory = new AttributesFactory(ra);
    factory.setConcurrencyChecksEnabled(true);
    if(!ra.getDataPolicy().isEmpty()) {
      factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
          5, EvictionAction.OVERFLOW_TO_DISK));
    }
    return factory.create();
  }
}
