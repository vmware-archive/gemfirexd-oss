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

package com.pivotal.gemfirexd.internal.engine.distributed.utils;

import com.gemstone.gemfire.internal.offheap.OffHeapRegionEntryHelper;
import com.gemstone.gemfire.internal.offheap.UnsafeMemoryChunk;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.gemstone.gemfire.pdx.internal.unsafe.UnsafeWrapper;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapByteSource;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapRegionEntryUtils;
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

public abstract class ServerResolverUtils extends ResolverUtils {

  private ServerResolverUtils() {
    // no instance
  }

  public static int addBytesToHashLarge(
      @Unretained final OffHeapByteSource bytes, int offset, final int length,
      int hash) {
    int currentBatchNum = 0;
    final int baseOffset = offset;
    final int batchSize = OffHeapRegionEntryUtils.calculateBatchSize(length);
    byte[] batch = new byte[batchSize];
    final int totalBatchNum = OffHeapRegionEntryUtils.calculateNumberOfBatches(
        length, batchSize);
    while (currentBatchNum < totalBatchNum) {
      final int size = OffHeapRegionEntryUtils.fillBatch(batch, bytes,
          currentBatchNum++, length, totalBatchNum, baseOffset);
      int i = 0;
      while (i < size) {
        hash ^= sbox[batch[i] & 0xff];
        hash *= 3;
        i++;
      }
    }
    return hash;
  }

  /**
   * The below hash generation uses substitution box technique to generate
   * hashcodes. This gives good distribution characteristics with nice
   * "avalanche" characteristics. The actual values in the {@link #sbox} array
   * are from {@link http://home.comcast.net/~bretm/hash/10.html}.
   */
  public static int addBytesToHash(@Unretained OffHeapByteSource bytes, int hash) {
    final UnsafeWrapper unsafe = UnsafeMemoryChunk.getUnsafeWrapper();
    final int length = bytes.getLength();
    return addBytesToHash(unsafe, bytes.getUnsafeAddress(0, length), length,
        hash);
  }

  /**
   * The below hash generation uses substitution box technique to generate
   * hashcodes. This gives good distribution characteristics with nice
   * "avalanche" characteristics. The actual values in the {@link #sbox} array
   * are from {@link http://home.comcast.net/~bretm/hash/10.html}.
   */
  public static int addBytesToHash(@Unretained final OffHeapByteSource bytes,
      final int offset, final int length, int hash) {
    // caching UnsafeWrapper and baseOffset for best performance
    return addBytesToHash(UnsafeMemoryChunk.getUnsafeWrapper(),
        bytes.getUnsafeAddress(offset, length), length, hash);
  }

  /**
   * The below hash generation uses substitution box technique to generate
   * hashcodes. This gives good distribution characteristics with nice
   * "avalanche" characteristics. The actual values in the {@link #sbox} array
   * are from {@link http://home.comcast.net/~bretm/hash/10.html}.
   */
  public static int addBytesToHash(final UnsafeWrapper unsafe, long memOffset,
      final int length, int hash) {
    final long endPos = (memOffset + length);
    // round off to nearest factor of 8 to read in longs
    final long endRound8Pos = (length % 8) != 0 ? (endPos - 8) : endPos;
    if (OffHeapRegionEntryHelper.NATIVE_BYTE_ORDER_IS_LITTLE_ENDIAN) {
      while (memOffset < endRound8Pos) {
        // splitting into longs is faster than reading one byte at a time even
        // though it costs more operations (about 20% in micro-benchmarks)
        final long v = unsafe.getLong(memOffset);
        hash ^= sbox[(int)(v & 0xff)];
        hash *= 3;
        hash ^= sbox[(int)((v >>> 8) & 0xff)];
        hash *= 3;
        hash ^= sbox[(int)((v >>> 16) & 0xff)];
        hash *= 3;
        hash ^= sbox[(int)((v >>> 24) & 0xff)];
        hash *= 3;
        hash ^= sbox[(int)((v >>> 32) & 0xff)];
        hash *= 3;
        hash ^= sbox[(int)((v >>> 40) & 0xff)];
        hash *= 3;
        hash ^= sbox[(int)((v >>> 48) & 0xff)];
        hash *= 3;
        hash ^= sbox[(int)((v >>> 56) & 0xff)];
        hash *= 3;

        memOffset += 8;
      }
    }
    else {
      while (memOffset < endRound8Pos) {
        // splitting into longs is faster than reading one byte at a time even
        // though it costs more operations (about 20% in micro-benchmarks)
        final long v = unsafe.getLong(memOffset);
        hash ^= sbox[(int)((v >>> 56) & 0xff)];
        hash *= 3;
        hash ^= sbox[(int)((v >>> 48) & 0xff)];
        hash *= 3;
        hash ^= sbox[(int)((v >>> 40) & 0xff)];
        hash *= 3;
        hash ^= sbox[(int)((v >>> 32) & 0xff)];
        hash *= 3;
        hash ^= sbox[(int)((v >>> 24) & 0xff)];
        hash *= 3;
        hash ^= sbox[(int)((v >>> 16) & 0xff)];
        hash *= 3;
        hash ^= sbox[(int)((v >>> 8) & 0xff)];
        hash *= 3;
        hash ^= sbox[(int)(v & 0xff)];
        hash *= 3;

        memOffset += 8;
      }
    }
    while (memOffset < endPos) {
      hash ^= sbox[unsafe.getByte(memOffset) & 0xff];
      hash *= 3;
      memOffset++;
    }
    return hash;
  }

  private static int addBytesToChunkedHash(final UnsafeWrapper unsafe,
      long memOffset, final int length, int hash) {
    final long endPos = (memOffset + length);
    while (memOffset < endPos) {
      int nextInt = 0;
      for (int i = 0; i < 4 && memOffset < endPos; i++) {
        nextInt <<= 8;
        nextInt += (unsafe.getByte(memOffset) & 0xFF);
        memOffset++;
      }
      hash = 31 * hash + nextInt;
    }
    return hash;
  }

  public static int addBytesToBucketHash(
      @Unretained final OffHeapByteSource bs, final int offset,
      final int length, int hash, final int typeId) {
    if (bs != null) {
      // caching UnsafeWrapper and baseOffset for best performance
      return addBytesToBucketHash(UnsafeMemoryChunk.getUnsafeWrapper(),
          bs.getUnsafeAddress(offset, length), length, hash, typeId);
    }
    else {
      return hash;
    }
  }

  public static int addBytesToBucketHash(final UnsafeWrapper unsafe,
      final long memOffset, final int length, int hash, final int typeId) {
    switch (typeId) {
      case StoredFormatIds.SQL_LONGINT_ID:
      case StoredFormatIds.SQL_INTEGER_ID:
      case StoredFormatIds.SQL_SMALLINT_ID:
      case StoredFormatIds.SQL_TINYINT_ID:
      case StoredFormatIds.LONGINT_TYPE_ID:
      case StoredFormatIds.INT_TYPE_ID:
      case StoredFormatIds.SMALLINT_TYPE_ID:
      case StoredFormatIds.TINYINT_TYPE_ID:
        return addBytesToChunkedHash(unsafe, memOffset, length, hash);
      default:
        return addBytesToHash(unsafe, memOffset, length, hash);
    }
  }
}
