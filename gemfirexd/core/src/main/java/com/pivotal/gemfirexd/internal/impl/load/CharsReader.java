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

package com.pivotal.gemfirexd.internal.impl.load;

import java.io.IOException;
import java.io.Reader;

/**
 * Like JDK's CharArrayReader but avoids all the locking. It uses char[] as
 * underlying data so care must be taken about mutability by callers. It also
 * allows for reusing the same reader by reseting with new underlying char[]s.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public final class CharsReader extends Reader {

  private char[] chars;
  private int len;
  private int cursor;
  private int mark;

  /**
   * Creates a new string reader wrapping around a given char[] skipping any
   * locking, so any synchronization should be done externally.
   */
  public CharsReader(char[] chars) {
    reset(chars, 0, chars.length);
  }

  /**
   * Creates a new string reader wrapping around a given char[] skipping any
   * locking, so any synchronization should be done externally.
   */
  public CharsReader(char[] chars, int offset, int len) {
    reset(chars, offset, len);
  }

  /**
   * Start reading from a new string.
   */
  public void reset(char[] chars, int offset, int len) {
    if (offset < 0 || len < 0) {
      throw new IllegalArgumentException();
    }
    this.chars = chars;
    this.len = len;
    this.cursor = offset;
    this.mark = -1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int read() {
    try {
      return this.chars[this.cursor++];
    } catch (ArrayIndexOutOfBoundsException e) {
      return -1;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read(char[] cbuf, int offset, int len) {
    int nCopy;
    if (len == 0) {
      return 0;
    }
    else if (len < 0 || (offset + len) > cbuf.length) {
      throw new IndexOutOfBoundsException();
    }
    else if ((nCopy = this.len - this.cursor) > 0) {
      if (nCopy > len) {
        nCopy = len;
      }
      System.arraycopy(this.chars, this.cursor, cbuf, offset, nCopy);
      this.cursor += nCopy;
      return nCopy;
    }
    else {
      return -1;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long skip(long ns) {
    if (this.cursor < this.len) {
      long n = Math.min(this.len - this.cursor, ns);
      n = Math.max(-this.cursor, n);
      this.cursor += n;
      return n;
    }
    else {
      return 0;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean ready() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean markSupported() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mark(int readAheadLimit) {
    if (readAheadLimit >= 0) {
      this.mark = this.cursor;
    }
    else {
      throw new IllegalArgumentException("Read-ahead limit < 0");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() throws IOException {
    if (this.mark >= 0) {
      this.cursor = this.mark;
      this.mark = -1;
    }
    else {
      throw new IOException("Stream not marked");
    }
  }

  public String asString() {
    return this.chars != null ? new String(this.chars) : null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    // move cursor beyond end of string
    this.cursor = this.len + 1;
  }
}
