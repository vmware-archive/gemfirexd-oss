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

package com.gemstone.gemfire.internal.shared.unsafe;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;


/**
 * A buffered DataInput abstraction over channel using direct byte buffers, and
 * using internal Unsafe class for best performance. Users must check for
 * {@link UnsafeHolder#getDirectByteBufferAddressMethod()} to be non-null before
 * trying to use this class.
 * <p>
 * The implementation is not thread-safe by design. This particular class can be
 * used as an efficient, buffered DataInput implementation for file channels,
 * socket channels and other similar.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public class ChannelBufferUnsafeDataInputStream extends
    ChannelBufferUnsafeInputStream implements DataInput {

  public ChannelBufferUnsafeDataInputStream(ReadableByteChannel channel)
      throws IOException {
    super(channel);
  }

  public ChannelBufferUnsafeDataInputStream(ReadableByteChannel channel,
      int bufferSize) throws IOException {
    super(channel, bufferSize);
  }

  /**
   * {@inheritDoc}
   */
  public final void readFully(byte[] b) throws IOException {
    readFully(b, 0, b.length);
  }

  /**
   * {@inheritDoc}
   */
  public final void readFully(byte[] b, int off, int len) throws IOException {
    while (true) {
      final int readBytes = super.read(b, off, len);
      if (readBytes >= len) {
        return;
      }
      else if (readBytes >= 0) {
        len -= readBytes;
        off += readBytes;
      }
      else {
        throw new EOFException();
      }
    }
  }

  /**
   * Currently not supported by this implementation.
   */
  public int skipBytes(int n) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   */
  public final boolean readBoolean() throws IOException {
    return readByte() != 0;
  }

  /**
   * {@inheritDoc}
   */
  public final byte readByte() throws IOException {
    final long addrPos = this.addrPosition;
    if (addrPos < this.addrLimit) {
      this.addrPosition++;
      return unsafe.getByte(addrPos);
    }
    else {
      refillBuffer(this.buffer, 1, "readByte: premature end of stream");
      return unsafe.getByte(this.addrPosition++);
    }
  }

  /**
   * {@inheritDoc}
   */
  public final int readUnsignedByte() throws IOException {
    return (readByte() & 0xFF);
  }

  /**
   * {@inheritDoc}
   */
  public final short readShort() throws IOException {
    final long addrPos = this.addrPosition;
    if ((this.addrLimit - addrPos) >= 2) {
      return getShort(addrPos);
    }
    else {
      refillBuffer(this.buffer, 2, "readShort: premature end of stream");
      return getShort(this.addrPosition);
    }
  }

  /**
   * {@inheritDoc}
   */
  public final int readUnsignedShort() throws IOException {
    return (readShort() & 0xFFFF);
  }

  /**
   * {@inheritDoc}
   */
  public final char readChar() throws IOException {
    final long addrPos = this.addrPosition;
    if ((this.addrLimit - addrPos) >= 2) {
      return (char)getShort(addrPos);
    }
    else {
      refillBuffer(this.buffer, 2, "readChar: premature end of stream");
      return (char)getShort(this.addrPosition);
    }
  }

  /**
   * {@inheritDoc}
   */
  public final long readLong() throws IOException {
    long addrPos = this.addrPosition;
    if ((this.addrLimit - addrPos) >= 8) {
      // more efficient to use getLong() instead of bytewise on most platforms
      this.addrPosition += 8;
      return this.buffer.getLong((int)(addrPos - this.baseAddress));
    }
    else {
      refillBuffer(this.buffer, 8, "readLong: premature end of stream");
      addrPos = this.addrPosition;
      this.addrPosition += 8;
      return this.buffer.getLong((int)(addrPos - this.baseAddress));
    }
  }

  /**
   * {@inheritDoc}
   */
  public final float readFloat() throws IOException {
    final long addrPos = this.addrPosition;
    if ((this.addrLimit - addrPos) >= 4) {
      return Float.intBitsToFloat(getInt(addrPos));
    }
    else {
      refillBuffer(this.buffer, 4, "readFloat: premature end of stream");
      return Float.intBitsToFloat(getInt(this.addrPosition));
    }
  }

  /**
   * {@inheritDoc}
   */
  public final double readDouble() throws IOException {
    long addrPos = this.addrPosition;
    if ((this.addrLimit - addrPos) >= 8) {
      // more efficient to use getLong() instead of bytewise on most platforms
      this.addrPosition += 8;
      return this.buffer.getDouble((int)(addrPos - this.baseAddress));
    }
    else {
      refillBuffer(this.buffer, 8, "readDouble: premature end of stream");
      addrPos = this.addrPosition;
      this.addrPosition += 8;
      return this.buffer.getDouble((int)(addrPos - this.baseAddress));
    }
  }

  /**
   * {@inheritDoc}
   */
  public String readLine() {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   */
  public String readUTF() throws IOException {
    return DataInputStream.readUTF(this);
  }

  protected final short getShort(long addrPos) {
    final sun.misc.Unsafe unsafe = ChannelBufferUnsafeInputStream.unsafe;
    int result = (unsafe.getByte(addrPos++) & 0xff);
    result = (result << 8) | (unsafe.getByte(addrPos++) & 0xff);
    this.addrPosition = addrPos;
    return (short)result;
  }
}
