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

package com.pivotal.gemfirexd.internal.engine.store;

import static com.gemstone.gemfire.internal.offheap.annotations.OffHeapIdentifier.COMPACT_COMPOSITE_KEY_VALUE_BYTES;

import com.gemstone.gemfire.internal.offheap.annotations.OffHeapIdentifier;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.gemstone.gemfire.internal.size.ReflectionSingleObjectSizer;
import com.pivotal.gemfirexd.internal.engine.sql.catalog.ExtraIndexInfo;

/**
 * A key which, when compared with other keys, takes a copy of the other key.
 * This key is used to perform a get on a skip list map and extract the actual
 * key object stored in the skip list map, hence the name ExtractingIndexKey.
 * 
 * NOTE: Keep this class as final since other places use .class instead of
 * instanceof check for performance reasons.
 * 
 * @author dsmith
 */
public final class ExtractingIndexKey extends CompactCompositeIndexKey {

  private static final long serialVersionUID = 1L;

  private CompactCompositeIndexKey foundKey;

  /**
   * The cached hashCode() calculated for this object.
   */
  private transient int hash;

  public ExtractingIndexKey(@Unretained(COMPACT_COMPOSITE_KEY_VALUE_BYTES) Object value, ExtraIndexInfo tabInfo) {
    super(value, tabInfo);
  }

  public CompactCompositeIndexKey getFoundKey() {
    return this.foundKey;
  }

  public void afterCompareWith(final CompactCompositeIndexKey otherKey) {
    this.foundKey = otherKey;
  }

  @Override
  public int hashCode() {
    int h = this.hash;
    if (h != 0) {
      return h;
    }
    else {
      return (this.hash = super.hashCode());
    }
  }

  @Override
  public long estimateMemoryUsage() {
    return super.estimateMemoryUsage() + 4 /*hash*/
        + ReflectionSingleObjectSizer.REFERENCE_SIZE;
  }
}
