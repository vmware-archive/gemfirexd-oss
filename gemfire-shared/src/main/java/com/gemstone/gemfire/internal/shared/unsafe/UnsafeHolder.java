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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import com.gemstone.gemfire.internal.shared.ChannelBufferFramedInputStream;
import com.gemstone.gemfire.internal.shared.ChannelBufferFramedOutputStream;
import com.gemstone.gemfire.internal.shared.ChannelBufferInputStream;
import com.gemstone.gemfire.internal.shared.ChannelBufferOutputStream;
import com.gemstone.gemfire.internal.shared.InputStreamChannel;
import com.gemstone.gemfire.internal.shared.OutputStreamChannel;

/**
 * Holder for static sun.misc.Unsafe instance and some convenience methods. Use
 * other methods only if {@link UnsafeHolder#hasUnsafe()} returns true;
 *
 * @author swale
 * @since gfxd 1.1
 */
public abstract class UnsafeHolder {

  private static final class Wrapper {

    static final sun.misc.Unsafe unsafe;

    static {
      sun.misc.Unsafe v;
      // try using "theUnsafe" field
      try {
        Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        field.setAccessible(true);
        v = (sun.misc.Unsafe)field.get(null);
      } catch (LinkageError le) {
        throw le;
      } catch (Throwable t) {
        throw new ExceptionInInitializerError(t);
      }
      if (v == null) {
        throw new ExceptionInInitializerError("theUnsafe not found");
      }
      unsafe = v;
    }

    static void init() {
    }
  }

  private static final boolean hasUnsafe;
  private static final Method directByteBufferAddressMethod;
  // Cached array base offset
  public static final long arrayBaseOffset;

  static {
    boolean v;
    long arrayOffset = -1;
    try {
      Wrapper.init();
      // try to access arrayBaseOffset via unsafe
      arrayOffset = (long)Wrapper.unsafe.arrayBaseOffset(byte[].class);
      v = true;
    } catch (LinkageError le) {
      le.printStackTrace();
      v = false;
    }
    hasUnsafe = v;
    arrayBaseOffset = arrayOffset;

    // check for "address()" method within DirectByteBuffer
    if (hasUnsafe) {
      Method m;
      ByteBuffer testBuf = ByteBuffer.allocateDirect(1);
      try {
        m = testBuf.getClass().getDeclaredMethod("address");
        m.setAccessible(true);
      } catch (Exception e) {
        m = null;
      }
      directByteBufferAddressMethod = m;
    } else {
      directByteBufferAddressMethod = null;
    }
  }

  private UnsafeHolder() {
    // no instance
  }

  public static boolean hasUnsafe() {
    return hasUnsafe;
  }

  public static Method getDirectByteBufferAddressMethod() {
    return directByteBufferAddressMethod;
  }

  public static sun.misc.Unsafe getUnsafe() {
    return Wrapper.unsafe;
  }

  @SuppressWarnings("resource")
  public static InputStreamChannel newChannelBufferInputStream(
      ReadableByteChannel channel, int bufferSize) throws IOException {
    return (directByteBufferAddressMethod != null
        ? new ChannelBufferUnsafeInputStream(channel, bufferSize)
        : new ChannelBufferInputStream(channel, bufferSize));
  }

  @SuppressWarnings("resource")
  public static OutputStreamChannel newChannelBufferOutputStream(
      WritableByteChannel channel, int bufferSize) throws IOException {
    return (directByteBufferAddressMethod != null
        ? new ChannelBufferUnsafeOutputStream(channel, bufferSize)
        : new ChannelBufferOutputStream(channel, bufferSize));
  }

  @SuppressWarnings("resource")
  public static InputStreamChannel newChannelBufferFramedInputStream(
      ReadableByteChannel channel, int bufferSize) throws IOException {
    return (directByteBufferAddressMethod != null
        ? new ChannelBufferUnsafeFramedInputStream(channel, bufferSize)
        : new ChannelBufferFramedInputStream(channel, bufferSize));
  }

  @SuppressWarnings("resource")
  public static OutputStreamChannel newChannelBufferFramedOutputStream(
      WritableByteChannel channel, int bufferSize) throws IOException {
    return (directByteBufferAddressMethod != null
        ? new ChannelBufferUnsafeFramedOutputStream(channel, bufferSize)
        : new ChannelBufferFramedOutputStream(channel, bufferSize));
  }

  // Maximum number of bytes to copy in one call of Unsafe's copyMemory.
  static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

  // Minimum size below which byte-wise copy is used instead of
  // Unsafe's copyMemory.
  static final int ARRAY_COPY_THRESHOLD = 6;

  /**
   * Copy from given source object to destination object.
   * <p/>
   *
   * @param src       source object; can be null in which case the
   *                  <code>srcOffset</code> must be a memory address
   * @param srcOffset offset in source object to start copy
   * @param dst       destination object; can be null in which case the
   *                  <code>dstOffset</code> must be a memory address
   * @param dstOffset destination address
   * @param length    number of bytes to copy
   */
  public static void copyMemory(final Object src, long srcOffset,
      final Object dst, long dstOffset, long length,
      final sun.misc.Unsafe unsafe) {
    while (length > 0) {
      long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
      unsafe.copyMemory(src, srcOffset, dst, dstOffset, size);
      length -= size;
      srcOffset += size;
      dstOffset += size;
    }
  }

  public static boolean checkBounds(int off, int len, int size) {
    return ((off | len | (off + len) | (size - (off + len))) >= 0);
  }

  /**
   * @see ByteBuffer#get(byte[], int, int)
   */
  public static void bufferGet(final byte[] dst, long address, int offset,
      final int length, final sun.misc.Unsafe unsafe) {
    if (length > ARRAY_COPY_THRESHOLD) {
      copyMemory(null, address, dst, arrayBaseOffset + offset, length, unsafe);
    } else {
      final int end = offset + length;
      while (offset < end) {
        dst[offset] = unsafe.getByte(address);
        address++;
        offset++;
      }
    }
  }

  /**
   * @see ByteBuffer#put(byte[], int, int)
   */
  public static void bufferPut(final byte[] src, long address, int offset,
      final int length, final sun.misc.Unsafe unsafe) {
    if (length > ARRAY_COPY_THRESHOLD) {
      copyMemory(src, arrayBaseOffset + offset, null, address, length, unsafe);
    } else {
      final int end = offset + length;
      while (offset < end) {
        unsafe.putByte(address, src[offset]);
        address++;
        offset++;
      }
    }
  }
}
