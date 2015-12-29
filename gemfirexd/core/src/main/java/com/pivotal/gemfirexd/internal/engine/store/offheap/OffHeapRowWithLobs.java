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

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.offheap.OffHeapRegionEntryHelper;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.ChunkType;
import com.gemstone.gemfire.internal.offheap.UnsafeMemoryChunk;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.gemstone.gemfire.pdx.internal.unsafe.UnsafeWrapper;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;

public final class OffHeapRowWithLobs extends OffHeapByteSource {

  public static final ChunkType TYPE = new ChunkType() {
    @Override
    public int getSrcType() {
      return Chunk.SRC_TYPE_WITH_LOBS;
    }

    @Override
    public Chunk newChunk(long memoryAddress) {
      return new OffHeapRowWithLobs(memoryAddress);
    }

    @Override
    public Chunk newChunk(long memoryAddress, int chunkSize) {
      return new OffHeapRowWithLobs(memoryAddress, chunkSize);
    }
  };

  /**
   * The numLobColumns field is used to store the number of lob columns along with
   * the width of the number of lob columns used to write to offheap.
   * The 2 MSB bits are used to store the width.
   * The encoding used to store the width is given by
   * bits 
   * 0x00    1 byte
   * 0x01    2 bytes
   * 0x02    3 bytes
   * 0x03    4 bytes   
   */
  private int numLobColumns = 0;
  private static final int LOB_COLUMNS_MASK = 0x3FFFFFFF;
  private static final int LOB_COLUMN_WIDTH_SHIFT = 30;  
  public static int LOB_ADDRESS_WIDTH = 8;

  public static int calcExtraChunkBytes(int numLobs) {
    return getUnsignedCompactIntNumBytes(numLobs)
        + (numLobs * LOB_ADDRESS_WIDTH);
  }

  private static int calcInMemoryNumLobCols(int numLobs, int lobWidth) {
    --lobWidth;
    int result = numLobs;
    result |= lobWidth << LOB_COLUMN_WIDTH_SHIFT;
    return result;
  }

  private OffHeapRowWithLobs(long address, int chunkSize) {
    super(address, chunkSize, TYPE);
  }

  private OffHeapRowWithLobs(long address) {
    super(address);
    this.initNumLobColumns();
  }

  public void initNumLobColumns() {
    long lobCountAddress = this.getBaseDataAddress() + this.getDataSize() - 1;
    int numLobs = readCompactNumLobs(lobCountAddress);
    int lobWidth = getUnsignedCompactIntNumBytes(numLobs);
    this.numLobColumns = calcInMemoryNumLobCols(numLobs, lobWidth);
  }

  public void setNumLobs(int numLobs) {
    if (numLobs <= 0) {
      throw new IllegalStateException("numLobs must be > 0 but it was "
          + numLobs);
    }
    else if (numLobs > LOB_COLUMNS_MASK) {
      throw new IllegalStateException("numLobs must be less than or equal to "
          + LOB_COLUMNS_MASK + "but it was " + numLobs);
    }
    // writeByte(0, (byte) numLobs);
    int lobWidth = writeCompactNumLobs(numLobs);
    this.numLobColumns = calcInMemoryNumLobCols(numLobs, lobWidth);
  }

  /**
   * 
   * @param numLobs
   * @return number of bytes written
   */
  private int writeCompactNumLobs(int numLobs) {
    int offset = this.getDataSize() - 1;
    int numBytes = 1;
    while (true) {
      if ((numLobs & ~0x7F) == 0) {
        writeByte(offset--, (byte)(numLobs & 0x7F));
        return numBytes;
      }
      else {
        writeByte(offset--, (byte)(numLobs & 0x7F | 0x80));
        numLobs >>>= 7;
        ++numBytes;
      }
    }
  }

  private static int readCompactNumLobs(@Unretained long bsAddr) {
    final UnsafeWrapper unsafe = UnsafeMemoryChunk.getUnsafeWrapper();
    byte b = unsafe.getByte(bsAddr);
    int result = b & 0x7F;
    int count = 0;
    while ((b & 0x80) != 0) {
      if (count > 4) {
        throw new GemFireXDRuntimeException("Malformed variable length integer");
      }
      b = unsafe.getByte(--bsAddr);
      result <<= 7;
      result |= b & 0x7F;
      ++count;
    }
    return result;
  }

  @Override
  public Object getDeserializedValue(Region r, RegionEntry re) {
    return this;
  }

  @Override
  public Object getValueAsDeserializedHeapObject() {
    return this.getRowByteArrays();
  }

  @Override
  public byte[] getValueAsHeapByteArray() {
    return getRawBytes();
  }

  @Override
  public int getSizeInBytes() {
    int size = super.getSizeInBytes();
    int numLobs = readNumLobsColumns(false);
    for (int i = 1; i <= numLobs; ++i) {
      long address = readAddressForLob(i);
      if (address != 0l && OffHeapRegionEntryHelper.isOffHeap(address)) {
        size += Chunk.getSize(address);
      }
    }
    return size;
  }

  @Override
  public boolean isWithLobs() {
    return true;
  }

  @Override
  public final int readNumLobsColumns(boolean throwExceptionOnWrongSource) {
    return this.numLobColumns & LOB_COLUMNS_MASK;
  }

  @Override
  public final int getOffsetAdjustment() {
    return (this.numLobColumns & LOB_COLUMNS_MASK) * LOB_ADDRESS_WIDTH;
  }

  @Override
  public final long getUnsafeAddress(int offset, int size) {
    return super.getUnsafeBaseAddress(offset + getOffsetAdjustment(), size);
  }

  @Override
  public final long getUnsafeAddress() {
    return this.memoryAddress + OFF_HEAP_HEADER_SIZE + getOffsetAdjustment();
  }

  @Override
  public final int getLength() {
    int lobColWidth = this.numLobColumns & ~LOB_COLUMNS_MASK;
    lobColWidth = lobColWidth >> LOB_COLUMN_WIDTH_SHIFT;
    ++lobColWidth;
    return getDataSize(UnsafeMemoryChunk.getUnsafeWrapper(), this.memoryAddress)
        - getOffsetAdjustment() - lobColWidth;
  }

  @Override
  public void toData(final DataOutput out) throws IOException {
    if (GemFireXDUtils.TraceRSIter) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
          "OffHeapRowWithLobs.toData: serializing offheap byte source=" + this);
    }
    final UnsafeWrapper unsafe = UnsafeMemoryChunk.getUnsafeWrapper();
    final int numArrays = readNumLobsColumns(false) + 1;
    final int baseRowLen = getLength();
    if (numArrays > 1 || baseRowLen > 0) {
      InternalDataSerializer.writeArrayLength(numArrays, out);
      // write first byte array
      serializeBaseRowBytes(unsafe, getBaseDataAddress()
          + getOffsetAdjustment(), baseRowLen, out);
      for (int index = 1; index < numArrays; index++) {
        serializeGfxdBytes(index, out);
      }
    }
    else {
      InternalDataSerializer.writeArrayLength(-1, out);
    }
    SimpleMemoryAllocatorImpl.getAllocator().getStats().incReads();
  }

  @Override
  public void sendTo(DataOutput out) throws IOException {
    out.writeByte(DSCODE.ARRAY_OF_BYTE_ARRAYS);
    toData(out);
  }

  @Override
  protected byte[] getRawBytes() {
    // TODO:Asif: Optimize the serialization of byte[][], by identifying the
    // size of total bytes needed and avoiding using
    // EntryEventImpl which uses HeapDataOutput
    // we need to serialize the byte[][]
    return EntryEventImpl.serialize(this);
  }

  private void writeLong(int offset, long value) {
    UnsafeMemoryChunk.writeAbsoluteLongVolatile(getBaseDataAddress() + offset,
        value);
  }

  public void setLobAddress(int i, long address) {
    writeLong((i - 1) * LOB_ADDRESS_WIDTH, address);
  }

  /**
   * Just get the length of each lob fields instead of creating a
   * OffHeapByteSource object. see ObjectSizer where a scan periodically tries
   * to estimate the valueSize.
   */
  public int getLobDataSizeLength(int index) {
    long address = readAddressForLob(index);
    if (address != 0x0L && OffHeapRegionEntryHelper.isOffHeap(address)) {
      return Chunk.getSize(address);
    }
    return 0;
  }

  @Override
  public final long readAddressForLob(int index) {
    assert index >= 1 && index <= readNumLobsColumns(true): "index=" + index
        + " numLobs=" + readNumLobsColumns(true);
    return UnsafeMemoryChunk.readAbsoluteLongVolatile(getBaseDataAddress()
        + (index - 1) * LOB_ADDRESS_WIDTH);
  }

  @Override
  public final long readAddressForLobIfPresent(int index) {
    return readAddressForLob(index);
  }

  // TODO: HOOTS: Find the matching retain
  @Released
  public static void freeLobsIfPresent(final long parentAddress,
      final ChunkType ct, int dataSizeDelta) {
    // read source type
    if (ct == OffHeapRowWithLobs.TYPE) {
      int dataSize = getSize(parentAddress) - dataSizeDelta;
      long startLocation = parentAddress + OFF_HEAP_HEADER_SIZE;
      int numLobCols = readCompactNumLobs(startLocation + dataSize - 1);
      for (int i = 1; i <= numLobCols; ++i, startLocation += LOB_ADDRESS_WIDTH) {
        long address = UnsafeMemoryChunk
            .readAbsoluteLongVolatile(startLocation);
        if (address != 0l && OffHeapRegionEntryHelper.isOffHeap(address)) {
          Chunk.release(address, false);
        }
      }
    }
  }

  @Override
  public String toString() {
    String string = this.toStringForOffHeapByteSource();
    byte[][] bytesbytes = this.getRowByteArrays();
    StringBuilder sb = new StringBuilder();
    sb.append(string);
    int i = 0;
    for (byte[] bytes : bytesbytes) {
      sb.append("<").append(i).append("th row bytes=");
      if (bytes != null) {
        sb.append(Arrays.toString(bytes));
      }
      else {
        sb.append("null");
      }
      sb.append(">");
      ++i;
    }
    return sb.toString();
  }
}
