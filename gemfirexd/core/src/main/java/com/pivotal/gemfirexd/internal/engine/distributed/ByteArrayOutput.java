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

package com.pivotal.gemfirexd.internal.engine.distributed;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * An expanding byte[] based OutputStream. No synchronization unlike
 * <code>ByteArrayOutputStream</code>.
 * 
 * @author swale
 * @since gfxd 1.4
 */
public class ByteArrayOutput extends OutputStream {

  protected byte[] buffer;
  protected int bufferPos;
  protected int bufferLen;

  public static final int DEFAULT_CAPACITY = 1024;

  public ByteArrayOutput() {
    this(DEFAULT_CAPACITY);
  }

  public ByteArrayOutput(final int allocSize) {
    this.buffer = new byte[allocSize < 32 ? 32 : allocSize];
    this.bufferLen = this.buffer.length;
  }

  public final int ensureCapacity(final int minCapacity, final int bufferPos) {
    if ((bufferPos + minCapacity) < this.bufferLen) {
      return bufferPos;
    }
    final int newLen = (bufferPos + minCapacity) + (this.bufferLen >> 2);
    this.buffer = Arrays.copyOf(this.buffer, newLen);
    this.bufferLen = newLen;
    return bufferPos;
  }

  @Override
  public final void write(final int b) {
    try {
      this.buffer[this.bufferPos++] = (byte)b;
      return;
    } catch (ArrayIndexOutOfBoundsException ae) {
      // expand the buffer
      ensureCapacity(1, this.bufferPos);
      this.buffer[this.bufferPos++] = (byte)b;
    }
  }

  @Override
  public final void write(final byte[] source, final int offset,
      final int len) {
    final int bufferPos = this.bufferPos;
    final int remaining = this.bufferLen - bufferPos;
    if (len <= remaining) {
      System.arraycopy(source, offset, this.buffer, bufferPos, len);
      this.bufferPos += len;
    }
    else {
      ensureCapacity(len, bufferPos);
      System.arraycopy(source, offset, this.buffer, this.bufferPos, len);
      this.bufferPos += len;
    }
  }

  @Override
  public final void write(final byte[] source) {
    this.write(source, 0, source.length);
  }

  /** Get a handle to the internal byte buffer */
  public final byte[] getData() {
    return this.buffer;
  }

  public final int position() {
    return this.bufferPos;
  }

  public final void advance(final int n) {
    this.bufferPos += n;
  }

  public final int size() {
    return this.bufferPos;
  }

  /**
   * Clear this ByteArrayOutput for reuse.
   */
  public final void clearForReuse() {
    this.bufferPos = 0;
  }

  @Override
  public void flush() {
    // no op
  }

  @Override
  public void close() {
    this.buffer = null;
    this.bufferPos = -1;
  }

  /**
   * Gets the contents of this stream as a byte[].
   */
  public final byte[] toByteArray() {
    return Arrays.copyOf(this.buffer, this.bufferPos);
  }

  /**
   * Write the contents of this stream to the specified byte buffer.
   * 
   * @throws BufferOverflowException
   *           if out is not large enough to contain all of the data
   */
  public final void sendTo(ByteBuffer out) {
    final int bufferPos = this.bufferPos;
    if (bufferPos > 0) {
      if (out.remaining() < bufferPos) {
        throw new BufferOverflowException();
      }
      out.put(this.buffer, 0, bufferPos);
    }
  }

  /**
   * Write the contents of this stream to the specified stream.
   */
  public final void sendTo(OutputStream out) throws IOException {
    final int bufferPos = this.bufferPos;
    if (bufferPos > 0) {
      out.write(this.buffer, 0, bufferPos);
    }
  }

  /**
   * Write the contents of this stream to the specified stream.
   * <p>
   * Note this implementation is exactly the same as writeTo(OutputStream) but
   * they do not both implement a common interface.
   */
  public final void sendTo(DataOutput out) throws IOException {
    final int bufferPos = this.bufferPos;
    if (bufferPos > 0) {
      out.write(this.buffer, 0, bufferPos);
    }
  }
}
