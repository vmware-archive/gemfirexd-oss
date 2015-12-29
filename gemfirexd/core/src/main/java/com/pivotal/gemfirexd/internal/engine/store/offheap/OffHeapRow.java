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

package com.pivotal.gemfirexd.internal.engine.store.offheap;

import java.util.Arrays;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.ChunkType;

public final class OffHeapRow extends OffHeapByteSource {

  public static final ChunkType TYPE = new ChunkType() {
    @Override
    public int getSrcType() {
      return Chunk.SRC_TYPE_NO_LOB_NO_DELTA;
    }

    @Override
    public Chunk newChunk(long memoryAddress) {
      return new OffHeapRow(memoryAddress);
    }

    @Override
    public Chunk newChunk(long memoryAddress, int chunkSize) {
      return new OffHeapRow(memoryAddress, chunkSize);
    }
  };

  private OffHeapRow(long address, int chunkSize) {
    super(address, chunkSize, TYPE);
  }

  private OffHeapRow(long address) {
    super(address);
  }

  @Override
  public int readNumLobsColumns(boolean throwExceptionOnWrongSource) {
    return 0;
  }

  @Override
  public Object getDeserializedValue(Region r, RegionEntry re) {
    return this;
  }

  @Override
  public Object getValueAsDeserializedHeapObject() {
    return this.getRowBytes();
  }

  @Override
  public String toString() {
    String string = this.toStringForOffHeapByteSource();
    byte[] bytes = this.getRowBytes();
    StringBuilder sb = new StringBuilder();
    sb.append(string);
    sb.append("<0th row bytes=");
    sb.append(Arrays.toString(bytes));
    sb.append(">");
    return sb.toString();
  }
}
