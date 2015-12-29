
/*

 Derived from source files from the Derby project.

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to you under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

/*
 This file was based on the MemStore patch written by Knut Magne, published
 under the Derby issue DERBY-2798 and released under the same license,
 ASF, as described above. The MemStore patch was in turn based on Derby source
 files.
 */

package com.pivotal.gemfirexd.internal.engine.access.operations;

import com.gemstone.gemfire.internal.util.ArrayUtils;
import com.pivotal.gemfirexd.internal.engine.access.index.MemIndex;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;

/**
 * Implement common functions called by the operations on CNM2Index.
 * 
 * @author yjing
 */
public abstract class MemIndexOperation extends MemOperation implements
    TxnIndexCommitAbortOperations {

  // the threshold for using RowLocation[] or ConcurrentHashMap.
  final static int ROWLOCATION_THRESHOLD = 100;

  protected final Object key;

  protected RowLocation row;
  

  protected IndexKeyAndRegionKey conflationKey;
  final public static Object TOK_INDEX_KEY_DEL= new Object(); 

  /**
   * This constructor is only used internally so that we do not need to clone
   * the input key and value.
   * 
   * @param container
   * @param key
   * @param value
   */
  protected MemIndexOperation(GemFireContainer container, Object key,
      RowLocation value) {
    super(container);
    this.key = key;
    this.row = value;
  }

  @Override
  public final boolean doAtCommitOrAbort() {
    return false;
  }

  @Override
  public boolean isIndexMemOperation() {
    return true;
  }
  
  public boolean isTransactionalOperation() {
    return false;
  }
  
  public TxnCommitAbortIndexAction getCommitAbortAction() {
    return null;
  }
  
  @Override
  public final Object getKeyToConflate() {
    if (this.conflationKey == null) {
      this.conflationKey = new IndexKeyAndRegionKey(this.key,
          this.row != null ? this.row.getKey() : null);
    }
    return this.conflationKey;
  }

  @Override
  public final Object getValueToConflate() {
    return this.row;
  }

  /**
   * Added to help debug and get more information at failure.
   */
  protected static void dumpIndex(GemFireContainer container, String marker) {
    ((MemIndex)container.getConglomerate())
        .dumpIndex("Unknown Data type in index");
  }

  static final class IndexKeyAndRegionKey implements
      Comparable<IndexKeyAndRegionKey> {

    private final Object indexKey;

    private final Object regionKey;

    IndexKeyAndRegionKey(Object indexKey, Object regionKey) {
      this.indexKey = indexKey;
      this.regionKey = regionKey;
    }

    public int compareTo(IndexKeyAndRegionKey other) {
      int res;
      if ((res = GemFireXDUtils.compareKeys(this.indexKey, other.indexKey)) != 0) {
        return res;
      }
      return GemFireXDUtils.compareKeys(this.regionKey, other.regionKey);
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof IndexKeyAndRegionKey) {
        IndexKeyAndRegionKey otherKey = (IndexKeyAndRegionKey)other;
        return ArrayUtils.objectEquals(this.indexKey, otherKey.indexKey)
            && ArrayUtils.objectEquals(this.regionKey, otherKey.regionKey);
      }
      return false;
    }
  }
}
