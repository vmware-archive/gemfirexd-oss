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
package com.pivotal.gemfirexd.internal.engine.access.operations;

import com.gemstone.gemfire.internal.concurrent.ConcurrentSkipListMap;
import com.gemstone.gemfire.internal.offheap.OffHeapHelper;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.store.CompactCompositeIndexKey;
import com.pivotal.gemfirexd.internal.engine.store.ExtractingIndexKey;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapByteSource;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

public class SortedMap2IndexRefreshIndexKeyOperation {

  public static boolean doMe(GemFireTransaction tran,
      GemFireContainer container, ExtractingIndexKey indexKey,
      RowLocation modifiedRow, @Unretained
      Object oldValue, boolean isLoadedFromHDFS, boolean isSuccess)
      throws StandardException {
    final ConcurrentSkipListMap<Object, Object> skipListMap = container
        .getSkipListMap();
    // bytesSameAs... method requires byte[] or ByteSource

    modifiedRow.markDeleteFromIndexInProgress();
    try {
      oldValue = SortedMap2IndexDeleteOperation.getRowByteSource(oldValue);
      @Retained
      @Released
      Object newValue = SortedMap2IndexDeleteOperation
          .getRowLocationByteSource(modifiedRow);
      try {
        Object valueBeingReplaced, replacementValue;
        if (isSuccess) {
          valueBeingReplaced = oldValue;
          replacementValue = newValue;
        } else {
          valueBeingReplaced = newValue;
          replacementValue = oldValue;
        }
        return basicRefreshIndexKey(indexKey, modifiedRow, valueBeingReplaced,
            replacementValue, isLoadedFromHDFS, skipListMap, isSuccess);

      } finally {
        OffHeapHelper.release(newValue);
      }
    } finally {
      modifiedRow.unmarkDeleteFromIndexInProgress();
    }

  }

  static boolean basicRefreshIndexKey(ExtractingIndexKey indexKey,
      RowLocation modifiedRow, @Retained
      @Unretained
      Object valueBeingReplaced, @Retained
      @Unretained
      Object replacementValue, boolean isLoadedFromHDFS,
      final ConcurrentSkipListMap<Object, Object> skipListMap, boolean isSuccess) {
    if (skipListMap.containsKey(indexKey)) {
      CompactCompositeIndexKey ccik = indexKey.getFoundKey();
      @Retained
      @Released
      Object vbs = ccik.getValueByteSource();
      try {
        if ((valueBeingReplaced != null && bytesSameAsCCIKBytes(
            valueBeingReplaced, vbs)) || valueBeingReplaced == null) {

          if (replacementValue != null && valueBeingReplaced != null) {
            ccik.update(replacementValue, valueBeingReplaced);
          } else if (replacementValue != null) {
            ccik.setValueBytes(replacementValue);
          }
        }
      } finally {
        ccik.releaseValueByteSource(vbs);
      }
      return true;
    } else {
      // in case of event loaded from HDFS , any previous or current value will
      // not be present in the index , so there will not be anything to refresh.
      // In that case we do not have to retain the value an extra time , even
      // if the index key to refresh is not found
      if (valueBeingReplaced instanceof Chunk && !isLoadedFromHDFS && isSuccess) {
        ((Chunk) valueBeingReplaced).retain();

        SanityManager.DEBUG_PRINT("Refresh Index key failed",
            "Refresh of index key for "
                + "unmodified index failed. Retaining the value bytes = "
                + valueBeingReplaced + "; This will produce an orphan");
      }
      return false;

    }

  }
  
  public static boolean forceSnapShotIndexKey(GemFireContainer container,
      ExtractingIndexKey indexKey, RowLocation rowLocation, 
      Object deletedBytes)
      throws StandardException {
    final ConcurrentSkipListMap<Object, Object> skipListMap = container
        .getSkipListMap();
    if (skipListMap.containsKey(indexKey)) {
      rowLocation.markDeleteFromIndexInProgress();
      CompactCompositeIndexKey ccik  = indexKey.getFoundKey();
      @Retained @Unretained Object indexBytes = ccik.getValueByteSource();
      try {       
        if (deletedBytes != null && SortedMap2IndexRefreshIndexKeyOperation
            .bytesSameAsCCIKBytes(deletedBytes, indexBytes)) {
          ccik.snapshotKeyFromValue();
        }
        return true;
      } finally {
        ccik.releaseValueByteSource(indexBytes);
        rowLocation.unmarkDeleteFromIndexInProgress();
      }
    } else {
      return false;
    }
  }

  public static boolean bytesSameAsCCIKBytes(@Unretained Object deletedBytes,
      Object ccikBytes) {
    if (deletedBytes == ccikBytes) {
      return true;
    }
    else if (deletedBytes instanceof OffHeapByteSource
        && ccikBytes instanceof OffHeapByteSource) {
      return deletedBytes.equals(ccikBytes);
    }
    else {
      return false;
    }
  }
}
