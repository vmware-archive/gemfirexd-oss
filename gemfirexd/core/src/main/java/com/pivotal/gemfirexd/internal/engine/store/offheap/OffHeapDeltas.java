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

import java.io.DataInput;
import java.io.IOException;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.ListOfDeltas;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.offheap.OffHeapRegionEntryHelper;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.ChunkType;
import com.gemstone.gemfire.internal.offheap.UnsafeMemoryChunk;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer.SerializableDelta;

public final class OffHeapDeltas extends OffHeapByteSource {
  public static final ChunkType TYPE = new ChunkType() {
    @Override
    public int getSrcType() {
      return Chunk.SRC_TYPE_WITH_MULTIPLE_DELTAS;
    }
    @Override
    public Chunk newChunk(long memoryAddress) {
      return new OffHeapDeltas(memoryAddress);
    }
    @Override
    public Chunk newChunk(long memoryAddress, int chunkSize) {
      return new OffHeapDeltas(memoryAddress, chunkSize);
    }
  };

  private OffHeapDeltas(long address, int chunkSize) {
    super(address, chunkSize, TYPE);
  }

  private OffHeapDeltas(long address) {
    super(address);
  }

  @Override
  public Object getDeserializedValue(Region r, RegionEntry re) {
    int numDeltas = this.readNumDeltas();
    ListOfDeltas lod = new ListOfDeltas(numDeltas);
    DataInput wrapper = this.getDataInputStreamWrapper(0, getLength());
    for (int i = 0; i < numDeltas; ++i) {
      SerializableDelta sd = new SerializableDelta();
      try {
        InternalDataSerializer.invokeFromData(sd, wrapper);
      } catch (IOException e) {
        throw new GemFireXDRuntimeException(e);
      } catch (ClassNotFoundException e) {
        throw new GemFireXDRuntimeException(e);
      }
      lod.merge(null, sd);
    }
    return lod;
  }

  @Override
  public int getSizeInBytes() {
    // TODO:Asif:The proper size for deltas is not returned and instead 0
    // is returned because of Bug 49019
    return 0;
  }

  private int readNumDeltas() {
    if (OffHeapRegionEntryHelper.NATIVE_BYTE_ORDER_IS_LITTLE_ENDIAN) {
      return Integer.reverseBytes(UnsafeMemoryChunk
          .readAbsoluteInt(getBaseDataAddress()));
    }
    else {
      return UnsafeMemoryChunk.readAbsoluteInt(getBaseDataAddress());
    }
  }

  @Override
  public int readNumLobsColumns(boolean throwExceptionOnWrongSource) {
    if (throwExceptionOnWrongSource) {
      throw new UnsupportedOperationException("readNumLobsColumns not suppoted on DELTAS");
    }
    return 0;
  }

  @Override
  public final long getUnsafeAddress(int offset, int size) {
    return super.getUnsafeBaseAddress(offset + 4, size);
  }

  @Override
  public final long getUnsafeAddress() {
    // add offsetAdjustment
    return super.getUnsafeAddress() + 4;
  }

  @Override
  public final int getLength() {
    return super.getLength() - 4;
  }

  @Override
  public final int getOffsetAdjustment() {
    return 4;
  }
}
