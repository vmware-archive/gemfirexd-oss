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

package com.gemstone.gemfire.internal;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UTFDataFormatException;

/**
 * Intermediate class to encapsulate common functionality of DataInput
 * implementation w.r.t. readUTF in particular.
 * 
 * @author swale
 * @since gfxd 2.0
 */
public abstract class DataInputStreamBase extends InputStream implements
    DataInput {

  protected int pos;
  /** reusable buffer for readUTF */
  protected char[] charBuf;

  protected DataInputStreamBase() {
  }

  protected DataInputStreamBase(int offset) {
    this.pos = offset;
  }

  protected final int skipOver(final long n, final int capacity) {
    final int remaining = (capacity - this.pos);
    if (n <= remaining) {
      this.pos += (int)n;
      return (int)n;
    }
    else {
      this.pos += remaining;
      return remaining;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String readLine() throws IOException {
    return new BufferedReader(new InputStreamReader(this)).readLine();
  }

  public final String readUTF(final byte[] bytes, final int nBytes)
      throws IOException {
    final int utfLen = readUnsignedShort();

    if ((this.pos + utfLen) <= nBytes) {
      if (this.charBuf == null || this.charBuf.length < utfLen) {
        int charBufLength = (((utfLen / 2) + 1) * 3);
        this.charBuf = new char[charBufLength];
      }
      final char[] chars = this.charBuf;

      int index = this.pos;
      final int limit = index + utfLen;
      int nChars = 0;
      int char1, char2, char3;

      // quick check for ASCII strings first
      for (; index < limit; index++, nChars++) {
        char1 = (bytes[index] & 0xff);
        if (char1 < 128) {
          chars[nChars] = (char)char1;
          continue;
        }
        else {
          break;
        }
      }

      for (; index < limit; index++, nChars++) {
        char1 = (bytes[index] & 0xff);
        // classify based on the high order 3 bits
        switch (char1 >> 5) {
          case 6:
            if ((index + 1) < limit) {
              // two byte encoding
              // 110yyyyy 10xxxxxx
              // use low order 6 bits of the next byte
              // It should have high order bits 10.
              char2 = bytes[++index];
              if ((char2 & 0xc0) == 0x80) {
                // 00000yyy yyxxxxxx
                chars[nChars] = (char)((char1 & 0x1f) << 6 | (char2 & 0x3f));
              }
              else {
                throwUTFEncodingError(index, char1, char2, null, 2);
              }
            }
            else {
              throw new UTFDataFormatException(
                  "partial 2-byte character at end (char1=" + char1 + ')');
            }
            break;
          case 7:
            if ((index + 2) < limit) {
              // three byte encoding
              // 1110zzzz 10yyyyyy 10xxxxxx
              // use low order 6 bits of the next byte
              // It should have high order bits 10.
              char2 = bytes[++index];
              if ((char2 & 0xc0) == 0x80) {
                // use low order 6 bits of the next byte
                // It should have high order bits 10.
                char3 = bytes[++index];
                if ((char3 & 0xc0) == 0x80) {
                  // zzzzyyyy yyxxxxxx
                  chars[nChars] = (char)(((char1 & 0x0f) << 12)
                      | ((char2 & 0x3f) << 6) | (char3 & 0x3f));
                }
                else {
                  throwUTFEncodingError(index, char1, char2, char3, 3);
                }
              }
              else {
                throwUTFEncodingError(index, char1, char2, null, 3);
              }
            }
            else {
              throw new UTFDataFormatException(
                  "partial 3-byte character at end (char1=" + char1 + ')');
            }
            break;
          default:
            // one byte encoding
            // 0xxxxxxx
            chars[nChars] = (char)char1;
            break;
        }
      }
      this.pos = limit;
      return new String(chars, 0, nChars);
    }
    else {
      throw new EOFException();
    }
  }

  protected void throwUTFEncodingError(int index, int char1, int char2,
      Integer char3, int enc) throws UTFDataFormatException {
    throw new UTFDataFormatException("malformed input for " + enc
        + "-byte encoding at " + index + " (char1=" + char1 + " char2=" + char2
        + (char3 == null ? ")" : (" char3=" + char3 + ')')));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    this.charBuf = null;
    this.pos = 0;
  }
}
