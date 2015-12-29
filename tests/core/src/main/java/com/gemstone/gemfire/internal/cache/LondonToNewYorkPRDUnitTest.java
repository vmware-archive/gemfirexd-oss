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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;

/** this test modifies the LondonToNewYorkDUnitTest to use a partitioned region.
 *
 *  @author Bruce Schuchardt
 *  @since 5.0
 */
public class LondonToNewYorkPRDUnitTest extends LondonToNewYorkDUnitTest {
  public LondonToNewYorkPRDUnitTest(String name) {
    super(name);
  }

  void setStoragePolicy(AttributesFactory factory) {
    factory.setPartitionAttributes((new PartitionAttributesFactory()).create());
  }
  
  void setParentRegionScope(AttributesFactory factory) {
    // PR doesn't support scope
  }
  /* the test creates a dynamic subregion of the main region, so we turn off
     dynamic region testing.  We could enable this if PRs supported subregions
   */
  boolean getTestDynamicRegions() {
    return false;
  }
}
