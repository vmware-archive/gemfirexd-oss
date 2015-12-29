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
package cacheperf.comparisons.parReg.fixed;

import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.FixedPartitionResolver;
import hydra.ConfigPrms;
import hydra.TestConfig;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * This resolver assumes keys are Longs, which is the case in all basic
 * scalability tests.
 */
public class KeyResolver implements FixedPartitionResolver {

  private static final List<String> partitionNames =
    TestConfig.getInstance().getRegionDescription(ConfigPrms.getRegionConfig())
              .getPartitionDescription().getFixedPartitionDescription()
              .getPartitionNames();
  
  private static final long size = partitionNames.size();
  
  public String getPartitionName(EntryOperation opDetails, Set unused) {
    Long key = (Long)opDetails.getKey();
    int partitionNum = (int)(key % size);
    return partitionNames.get(partitionNum);
  }

  public String getName() {
    return "KeyResolver";
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    return (Serializable)opDetails.getKey();
  }

  public void close() {
  }
}
