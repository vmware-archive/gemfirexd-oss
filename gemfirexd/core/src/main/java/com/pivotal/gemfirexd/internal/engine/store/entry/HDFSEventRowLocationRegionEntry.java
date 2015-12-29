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

import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.RegionEntryContext;

public class HDFSEventRowLocationRegionEntry extends
    VMLocalRowLocationThinRegionEntryHeap {

  private final byte[] rawKey;
  private final PersistedEventImpl event;

  public HDFSEventRowLocationRegionEntry(RegionEntryContext context,
      byte[] key, PersistedEventImpl event) {
    super(context, EntryEventImpl.deserialize(key), event.getValue());
    this.rawKey = key;
    this.event = event;
  }

  public PersistedEventImpl getEvent() {
    return event;
  }

  public byte[] getRawKeyBytes() {
    return rawKey;
  }
}
