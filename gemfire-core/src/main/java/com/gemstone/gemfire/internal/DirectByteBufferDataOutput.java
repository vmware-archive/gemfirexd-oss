/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

package com.gemstone.gemfire.internal;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.gemstone.gemfire.internal.shared.ClientSharedUtils;
import com.gemstone.gemfire.internal.shared.Version;
import com.gemstone.gemfire.internal.shared.unsafe.ChannelBufferUnsafeDataOutputStream;
import com.gemstone.gemfire.internal.shared.unsafe.UnsafeHolder;

/**
 * Implements {@link DataOutput} writing to a direct ByteBuffer
 * expanding it as required.
 * <p>
 * Note: this can be further optimized by using the Unsafe API rather than
 * going through ByteBuffer API (e.g. see ChannelBufferUnsafeDataOutputStream)
 * but won't have an effect for large byte array writes (like for column data)
 */
public final class DirectByteBufferDataOutput extends ByteBufferOutput
    implements DataOutput, Closeable, VersionedDataStream {

  private static final int INITIAL_SIZE = 1024;

  private final Version version;

  public DirectByteBufferDataOutput(Version version) {
    this(INITIAL_SIZE, version);
  }

  public DirectByteBufferDataOutput(int initialSize, Version version) {
    super(initialSize, Integer.MAX_VALUE); // no max limit
    this.version = version;
  }

  @Override
  public Version getVersion() {
    return this.version;
  }

  @Override
  public void writeChar(int v) throws IOException {
    super.writeChar((char)v);
  }

  @Override
  public void writeUTF(@Nonnull String s) throws IOException {
    // kryo's writeString is not DataOutput UTF8 compatible

    // first calculate the UTF encoded length
    final int strLen = s.length();
    final int utfLen = ClientSharedUtils.getUTFLength(s, strLen);
    if (utfLen > 65535) {
      throw new UTFDataFormatException("encoded string too long: " + utfLen
          + " bytes");
    }
    // make required space
    require(utfLen + 2);
    final ByteBuffer buffer = this.niobuffer;
    // write the length first
    buffer.putShort((short)utfLen);
    position += 2;
    // now write as UTF data using unsafe API
    final long address = UnsafeHolder.getDirectBufferAddress(buffer);
    ChannelBufferUnsafeDataOutputStream.writeUTFSegmentNoOverflow(s, 0, strLen,
        address + position, UnsafeHolder.getUnsafe());
    position += utfLen;
    buffer.position(position);
  }

  @Override
  public void writeBytes(@Nonnull String s) throws IOException {
    int len = s.length();
    require(len);
    for (int i = 0; i < len; i++) {
      niobuffer.put((byte)s.charAt(i));
      position++;
    }
  }

  @Override
  public void writeChars(@Nonnull String s) throws IOException {
    int len = s.length();
    final int required = len << 1;
    if (required < 0) {
      throw new KryoException("Buffer overflow with required=" + required);
    }
    require(required);
    for (int i = 0; i < len; i++) {
      niobuffer.putShort((short)s.charAt(i));
      position += 2;
    }
  }

  @Override
  protected boolean require(int required) throws KryoException {
    if (capacity - position >= required) return false;
    capacity = Math.max((int)Math.min((long)capacity << 1L,
        Integer.MAX_VALUE), position + required);
    niobuffer.rewind();
    niobuffer.limit(position);
    // reallocation will do full copy first time around since allocation is
    // not using UnsafeHolder.allocateDirectBuffer but next time onwards it
    // will use the efficient C realloc() call that avoids copying if possible
    ByteBuffer newBuffer = UnsafeHolder.reallocateDirectBuffer(
        niobuffer, capacity);
    // set the position of newBuffer
    newBuffer.position(position);
    setBuffer(newBuffer, Integer.MAX_VALUE);
    return true;
  }
}
