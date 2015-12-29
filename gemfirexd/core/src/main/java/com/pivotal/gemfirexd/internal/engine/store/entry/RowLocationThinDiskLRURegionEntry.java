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
package com.pivotal.gemfirexd.internal.engine.store.entry;

import com.gemstone.gemfire.internal.cache.RegionEntryContext;
import com.gemstone.gemfire.internal.cache.VMThinDiskLRURegionEntry;
import com.gemstone.gemfire.internal.cache.lru.Sizeable;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;

public abstract class RowLocationThinDiskLRURegionEntry extends VMThinDiskLRURegionEntry implements RowLocation, Sizeable {
  public RowLocationThinDiskLRURegionEntry(RegionEntryContext context,
      Object value) {
    super(context, value);
  }

  /////////////////////////////////////////////////////////////
  /////////////////////////// fields //////////////////////////
  /////////////////////////////////////////////////////////////
  // Do not add any instance fields to this class.
  // Instead add them to the ROWLOCATION section of LeafRegionEntry.cpp
}
