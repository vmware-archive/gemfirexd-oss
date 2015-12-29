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
package com.gemstone.gemfire.cache.snapshot;

import java.io.Serializable;
import java.util.Map.Entry;

/**
 * Filters region entries for inclusion into a snapshot.
 * 
 * @param <K> the key type
 * @param <V> the value type
 * 
 * @see CacheSnapshotService
 * @see RegionSnapshotService
 * 
 * @author bakera
 * @since 7.0
 */
public interface SnapshotFilter<K, V> extends Serializable {
  /**
   * Returns true if the entry passes the filter criteria; false otherwise.  Invoking 
   * <code>Entry.getValue</code> may incur additional overhead to deserialize the
   * entry value.
   * 
   * @param entry the cache entry to evaluate
   * @return true if the entry is accepted by the filter
   */
  boolean accept(Entry<K, V> entry);
}
  
