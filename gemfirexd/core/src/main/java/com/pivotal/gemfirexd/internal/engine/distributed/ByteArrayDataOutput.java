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
import java.io.UTFDataFormatException;

import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;

/**
 * An expanding byte[] based DataOutputStream. Best used as intermediate buffer
 * instead of <code>DataOutputStream</code> over a channel or something and if
 * this can be reused.
 * 
 * @author swale
 * @since gfxd 1.4
 */
public final class ByteArrayDataOutput extends ByteArrayOutput implements
    DataOutput {

  // to include this class in gemfirexd.jar
  static void init() {
  }

  public ByteArrayDataOutput() {
    super();
  }

  public ByteArrayDataOutput(final int allocSize) {
    super(allocSize);
  }

  @Override
  public final void writeBoolean(final boolean v) {
    super.write(v ? 1 : 0);
  }

  @Override
  public final void writeByte(final int v) {
    super.write(v);
  }

  @Override
  public final void writeShort(final int v) {
    int pos = ensureCapacity(2, this.bufferPos);

    final byte[] buffer = this.buffer;
    buffer[pos++] = (byte)((v >>> 8) & 0xff);
    buffer[pos++] = (byte)(v & 0xff);
    this.bufferPos = pos;
  }

  @Override
  public final void writeChar(final int v) {
    writeShort(v);
  }

  @Override
  public final void writeInt(final int v) {
    int pos = ensureCapacity(4, this.bufferPos);

    final byte[] buffer = this.buffer;
    buffer[pos++] = (byte)((v >>> 24) & 0xff);
    buffer[pos++] = (byte)((v >>> 16) & 0xff);
    buffer[pos++] = (byte)((v >>> 8) & 0xff);
    buffer[pos++] = (byte)(v & 0xff);
    this.bufferPos = pos;
  }

  @Override
  public final void writeLong(final long v) {
    int pos = ensureCapacity(8, this.bufferPos);

    final byte[] buffer = this.buffer;
    buffer[pos++] = (byte)((v >>> 56) & 0xff);
    buffer[pos++] = (byte)((v >>> 48) & 0xff);
    buffer[pos++] = (byte)((v >>> 40) & 0xff);
    buffer[pos++] = (byte)((v >>> 32) & 0xff);
    buffer[pos++] = (byte)((v >>> 24) & 0xff);
    buffer[pos++] = (byte)((v >>> 16) & 0xff);
    buffer[pos++] = (byte)((v >>> 8) & 0xff);
    buffer[pos++] = (byte)(v & 0xff);
    this.bufferPos = pos;
  }

  @Override
  public final void writeFloat(final float v) {
    writeInt(Float.floatToIntBits(v));
  }

  @Override
  public final void writeDouble(final double v) {
    writeLong(Double.doubleToLongBits(v));
  }

  /** assumes strlen == str.length */
  final void writeBytes(final char[] str, final int strlen) {
    int pos = ensureCapacity(strlen, this.bufferPos);

    final byte[] buffer = this.buffer;
    for (char c : str) {
      buffer[pos++] = (byte)c;
    }
    this.bufferPos = pos;
  }

  public final void writeBytes(final char[] str, int offset, final int end) {
    int pos = ensureCapacity(end - offset, this.bufferPos);

    final byte[] buffer = this.buffer;
    while (offset < end) {
      buffer[pos++] = (byte)str[offset++];
    }
    this.bufferPos = pos;
  }

  public final void writeBytes(final String str, final int strlen) {
    final char[] chars = ResolverUtils.getInternalCharsOnly(str, strlen);
    if (chars != null) {
      writeBytes(chars, strlen);
      return;
    }

    int pos = ensureCapacity(strlen, this.bufferPos);

    final byte[] buffer = this.buffer;
    for (int i = 0; i < strlen; i++) {
      buffer[pos++] = (byte)str.charAt(i);
    }
    this.bufferPos = pos;
  }

  @Override
  public final void writeBytes(final String str) {
    writeBytes(str, str.length());
  }

  @Override
  public final void writeChars(final String str) {
    final int strlen = str.length();
    int pos = ensureCapacity((strlen << 1), this.bufferPos);

    final byte[] buffer = this.buffer;
    for (int i = 0; i < strlen; i++) {
      final int c = str.charAt(i);
      buffer[pos++] = (byte)((c >>> 8) & 0xff);
      buffer[pos++] = (byte)(c & 0xff);
    }
    this.bufferPos = pos;
  }

  public static final int getUTFLength(final char[] str, final int strlen) {
    int utflen = strlen;
    for (char c : str) {
      if ((c >= 0x0001) && (c <= 0x007F)) {
        // 1 byte for character
        continue;
      }
      else if (c > 0x07FF) {
        utflen += 2; // 3 bytes for character
      }
      else {
        utflen++; // 2 bytes for character
      }
    }
    return utflen;
  }

  public static final int getUTFLength(final String str, final int strlen) {
    int utflen = strlen;
    for (int i = 0; i < strlen; i++) {
      final char c = str.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        // 1 byte for character
        continue;
      }
      else if (c > 0x07FF) {
        utflen += 2; // 3 bytes for character
      }
      else {
        utflen++; // 2 bytes for character
      }
    }
    return utflen;
  }

  /** assumes strlen == str.length */
  final void writeUTFNoLength(final char[] str, final int strlen,
      final int utflen) {
    if (strlen == utflen) {
      // quick path for ASCII string
      writeBytes(str, strlen);
    }
    else {
      int pos = ensureCapacity(utflen, this.bufferPos);

      final byte[] buffer = this.buffer;
      for (char c : str) {
        if ((c >= 0x0001) && (c <= 0x007F)) {
          buffer[pos++] = (byte)(c & 0xFF);
        }
        else if (c > 0x07FF) {
          buffer[pos++] = (byte)(0xE0 | ((c >> 12) & 0x0F));
          buffer[pos++] = (byte)(0x80 | ((c >> 6) & 0x3F));
          buffer[pos++] = (byte)(0x80 | ((c >> 0) & 0x3F));
        }
        else {
          buffer[pos++] = (byte)(0xC0 | ((c >> 6) & 0x1F));
          buffer[pos++] = (byte)(0x80 | ((c >> 0) & 0x3F));
        }
      }
      this.bufferPos = pos;
    }
  }

  public final void writeUTFNoLength(final String str, final int strlen,
      final int utflen) {
    if (strlen == utflen) {
      // quick path for ASCII string
      writeBytes(str, strlen);
    }
    else {
      int pos = ensureCapacity(utflen, this.bufferPos);

      final byte[] buffer = this.buffer;
      for (int i = 0; i < strlen; i++) {
        final char c = str.charAt(i);
        if ((c >= 0x0001) && (c <= 0x007F)) {
          buffer[pos++] = (byte)(c & 0xFF);
        }
        else if (c > 0x07FF) {
          buffer[pos++] = (byte)(0xE0 | ((c >> 12) & 0x0F));
          buffer[pos++] = (byte)(0x80 | ((c >> 6) & 0x3F));
          buffer[pos++] = (byte)(0x80 | ((c >> 0) & 0x3F));
        }
        else {
          buffer[pos++] = (byte)(0xC0 | ((c >> 6) & 0x1F));
          buffer[pos++] = (byte)(0x80 | ((c >> 0) & 0x3F));
        }
      }
      this.bufferPos = pos;
    }
  }

  public static final int getUTFLength(final String str) {
    final int strlen = str.length();
    final char[] chars = ResolverUtils.getInternalCharsOnly(str, strlen);
    if (chars != null) {
      return getUTFLength(chars, strlen);
    }
    else {
      return getUTFLength(str, strlen);
    }
  }

  public final void writeUTFNoLength(final String str, final int utflen) {
    final int strlen = str.length();
    final char[] chars = ResolverUtils.getInternalCharsOnly(str, strlen);
    if (chars != null) {
      writeUTFNoLength(chars, strlen, utflen);
    }
    else {
      writeUTFNoLength(str, strlen, utflen);
    }
  }

  public final void writeUTFNoLength(final String str) {
    final int strlen = str.length();
    final char[] chars = ResolverUtils.getInternalCharsOnly(str, strlen);
    if (chars != null) {
      final int utflen = getUTFLength(chars, strlen);
      writeUTFNoLength(chars, strlen, utflen);
    }
    else {
      final int utflen = getUTFLength(str, strlen);
      writeUTFNoLength(str, strlen, utflen);
    }
  }

  @Override
  public final void writeUTF(final String str) throws UTFDataFormatException {
    final int strlen = str.length();
    final char[] chars = ResolverUtils.getInternalCharsOnly(str, strlen);
    if (chars != null) {
      final int utflen = getUTFLength(chars, strlen);

      if (utflen <= 65535) {
        writeShort(utflen);
        writeUTFNoLength(chars, strlen, utflen);
      }
      else {
        throw new UTFDataFormatException("ByteArrayDataOutput#writeUTF: "
            + "encoded string too long: " + utflen + " bytes");
      }
    }
    else {
      final int utflen = getUTFLength(str, strlen);

      if (utflen <= 65535) {
        writeShort(utflen);
        writeUTFNoLength(str, strlen, utflen);
      }
      else {
        throw new UTFDataFormatException("ByteArrayDataOutput#writeUTF: "
            + "encoded string too long: " + utflen + " bytes");
      }
    }
  }
}
