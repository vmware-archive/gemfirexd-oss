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

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.channels.WritableByteChannel;


/**
 * A buffered DataOutput abstraction over channel using direct byte buffers, and
 * using internal Unsafe class for best performance. Users must check for
 * {@link UnsafeHolder#getDirectByteBufferAddressMethod()} to be non-null before
 * trying to use this class.
 * <p>
 * The implementation is not thread-safe by design. This particular class can be
 * used as an efficient, buffered DataOutput implementation for file channels,
 * socket channels and other similar.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public class ChannelBufferUnsafeDataOutputStream extends
    ChannelBufferUnsafeOutputStream implements DataOutput {

  public ChannelBufferUnsafeDataOutputStream(WritableByteChannel channel)
      throws IOException {
    super(channel);
  }

  public ChannelBufferUnsafeDataOutputStream(WritableByteChannel channel,
      int bufferSize) throws IOException {
    super(channel, bufferSize);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeBoolean(boolean v) throws IOException {
    super.write(v ? 1 : 0);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeByte(int v) throws IOException {
    super.write(v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeShort(int v) throws IOException {
    final long addrPos = this.addrPosition;
    if ((this.addrLimit - addrPos) >= 2) {
      this.addrPosition = putShort(addrPos, v);
    }
    else {
      flushBufferBlocking(this.buffer);
      this.addrPosition = putShort(this.addrPosition, v);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeChar(int v) throws IOException {
    writeShort(v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeInt(int v) throws IOException {
    final long addrPos = this.addrPosition;
    if ((this.addrLimit - addrPos) >= 4) {
      this.addrPosition = putInt(addrPos, v);
    }
    else {
      flushBufferBlocking(this.buffer);
      this.addrPosition = putInt(this.addrPosition, v);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeLong(long v) throws IOException {
    long addrPos = this.addrPosition;
    if ((this.addrLimit - addrPos) >= 8) {
      // more efficient to use putLong() instead of bytewise on most platforms
      this.addrPosition += 8;
      this.buffer.putLong((int)(addrPos - this.baseAddress), v);
    }
    else {
      flushBufferBlocking(this.buffer);
      addrPos = this.addrPosition;
      this.addrPosition += 8;
      this.buffer.putLong((int)(addrPos - this.baseAddress), v);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeFloat(float v) throws IOException {
    writeInt(Float.floatToIntBits(v));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeDouble(double v) throws IOException {
    writeLong(Double.doubleToLongBits(v));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeBytes(String s) throws IOException {
    final sun.misc.Unsafe unsafe = ChannelBufferUnsafeOutputStream.unsafe;
    int off = 0;
    int len = s.length();
    while (len > 0) {
      long addrPos = this.addrPosition;
      final int remaining = (int)(this.addrLimit - addrPos);
      if (len <= remaining) {
        final int end = (off + len);
        while (off < end) {
          unsafe.putByte(addrPos, (byte)(s.charAt(off) & 0xff));
          addrPos++;
          off++;
        }
        this.addrPosition = addrPos;
        return;
      }
      else {
        final int end = (off + remaining);
        while (off < end) {
          unsafe.putByte(addrPos, (byte)(s.charAt(off) & 0xff));
          addrPos++;
          off++;
        }
        this.addrPosition = addrPos;
        flushBufferBlocking(this.buffer);
        len -= remaining;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeChars(String s) throws IOException {
    final sun.misc.Unsafe unsafe = ChannelBufferUnsafeOutputStream.unsafe;
    int off = 0;
    int len = s.length();
    while (len > 0) {
      long addrPos = this.addrPosition;
      final int remaining = (int)(this.addrLimit - addrPos);
      if ((len << 1) <= remaining) {
        final int end = (off + len);
        int c;
        while (off < end) {
          c = s.charAt(off++);
          unsafe.putByte(addrPos++, (byte)((c >>> 8) & 0xff));
          unsafe.putByte(addrPos++, (byte)(c & 0xff));
        }
        this.addrPosition = addrPos;
        return;
      }
      else {
        final int remchars = (remaining >>> 1);
        final int end = (off + remchars);
        int c;
        while (off < end) {
          c = s.charAt(off++);
          unsafe.putByte(addrPos++, (byte)((c >>> 8) & 0xff));
          unsafe.putByte(addrPos++, (byte)(c & 0xff));
        }
        this.addrPosition = addrPos;
        flushBufferBlocking(this.buffer);
        len -= remchars;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeUTF(String str) throws IOException {
    final sun.misc.Unsafe unsafe = ChannelBufferUnsafeOutputStream.unsafe;
    int strlen = str.length();
    if (strlen > 65535) {
      throw new UTFDataFormatException("encoded string too long: " + strlen);
    }

    // first check the optimistic case where worst case of 2 for length + 3 for
    // each char fits into remaining space in buffer
    long addrPos = this.addrPosition;
    long remaining = this.addrLimit - addrPos;
    if (remaining >= ((strlen * 3) + 2)) {
      // write the UTF string skipping the length, then write length at the last
      addrPos += 2;
      final long finalAddrPos = writeUTFSegmentNoOverflow(str, 0, strlen,
          addrPos, unsafe);
      long utflen = (finalAddrPos - addrPos);
      if (utflen <= 65535) {
        addrPos -= 2;
        unsafe.putByte(addrPos++, (byte)((utflen >>> 8) & 0xff));
        unsafe.putByte(addrPos++, (byte)(utflen & 0xff));
        this.addrPosition = finalAddrPos;
      }
      else {
        // act as if we wrote nothing to this buffer (no change to addrPosition)
        throw new UTFDataFormatException("encoded string too long: " + utflen
            + " bytes");
      }
      return;
    }

    // otherwise first calculate the UTF encoded length, write it in buffer
    // (which may need to be flushed at any point), then break string into worst
    // case segments for writing to buffer and flushing if end of buffer reached
    int utflen = 0;
    int c;
    for (int index = 0; index < strlen; index++) {
      c = str.charAt(index);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        utflen++;
      }
      else if (c > 0x07FF) {
        utflen += 3;
      }
      else {
        utflen += 2;
      }
    }
    if (utflen > 65535) {
      throw new UTFDataFormatException("encoded string too long: " + utflen
          + " bytes");
    }
    // write the length first
    if (remaining > 2) {
      addrPos = putShort(addrPos, utflen);
      remaining -= 2;
    }
    else {
      flushBufferBlocking(this.buffer);
      addrPos = putShort(this.addrPosition, utflen);
      remaining = this.addrLimit - addrPos;
    }

    // next break string into segments assuming worst case, flushing buffer as
    // required after each segment write
    int offset = 0;
    while (strlen > 0) {
      int writeLen = (int)(remaining / 3);
      if (writeLen >= 3) {
        // write the UTF segment and update the number of remaining characters,
        // offset, remaining buffer size etc
        long newAddrPos = writeUTFSegmentNoOverflow(str, offset, writeLen,
            addrPos, unsafe);
        strlen -= writeLen;
        offset += writeLen;
        remaining -= (newAddrPos - addrPos);
        addrPos = newAddrPos;
      }
      else {
        // if we have too few to write then better to flush the buffer and then
        // try (bufferSize is at least 10 as ensured in constructors)
        this.addrPosition = addrPos;
        flushBufferBlocking(this.buffer);
        remaining = this.addrLimit - (addrPos = this.addrPosition);
      }
    }
  }

  private static final long writeUTFSegmentNoOverflow(String str, int offset,
      int length, long addrPos, final sun.misc.Unsafe unsafe)
      throws IOException {
    final int end = (offset + length);
    while (offset < end) {
      int c = str.charAt(offset);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        unsafe.putByte(addrPos++, (byte)c);
      }
      else if (c > 0x07FF) {
        unsafe.putByte(addrPos++, (byte)(0xE0 | ((c >> 12) & 0x0F)));
        unsafe.putByte(addrPos++, (byte)(0x80 | ((c >> 6) & 0x3F)));
        unsafe.putByte(addrPos++, (byte)(0x80 | ((c >> 0) & 0x3F)));
      }
      else {
        unsafe.putByte(addrPos++, (byte)(0xC0 | ((c >> 6) & 0x1F)));
        unsafe.putByte(addrPos++, (byte)(0x80 | ((c >> 0) & 0x3F)));
      }
      offset++;
    }
    return addrPos;
  }

  protected static final long putShort(long addrPos, final int v) {
    final sun.misc.Unsafe unsafe = ChannelBufferUnsafeOutputStream.unsafe;
    unsafe.putByte(addrPos++, (byte)((v >>> 8) & 0xff));
    unsafe.putByte(addrPos++, (byte)(v & 0xff));
    return addrPos;
  }

  protected static final long putInt(long addrPos, final int v) {
    final sun.misc.Unsafe unsafe = ChannelBufferUnsafeOutputStream.unsafe;
    unsafe.putByte(addrPos++, (byte)((v >>> 24) & 0xff));
    unsafe.putByte(addrPos++, (byte)((v >>> 16) & 0xff));
    unsafe.putByte(addrPos++, (byte)((v >>> 8) & 0xff));
    unsafe.putByte(addrPos++, (byte)(v & 0xff));
    return addrPos;
  }
}
