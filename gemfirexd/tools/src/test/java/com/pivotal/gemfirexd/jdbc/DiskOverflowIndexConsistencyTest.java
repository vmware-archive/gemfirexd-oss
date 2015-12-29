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
package com.pivotal.gemfirexd.jdbc;

import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;

public class DiskOverflowIndexConsistencyTest extends IndexConsistencyTest
{

  public DiskOverflowIndexConsistencyTest(String name) {
    super(name);    
  }
  @Override
  public String getSuffix() {
    return  " eviction by lrucount 1 evictaction overflow synchronous ";
  }

  @Override
  protected void additionalTableAttributes(
      RegionAttributesCreation expectedAttrs) {
    expectedAttrs.setEvictionAttributes(EvictionAttributes
        .createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
    expectedAttrs.setDiskStoreName(GfxdConstants.GFXD_DEFAULT_DISKSTORE_NAME);
  }
}
