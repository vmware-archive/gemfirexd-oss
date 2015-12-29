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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A wrapper {@link FileInputStream} that will read only till the given position
 * in the underlying {@link FileInputStream}.
 * 
 * @author swale
 * @since 7.0
 */
public class PartialFileInputStream extends FileInputStream {

  private long numBytesToRead;

  /**
   * Creates a new InputStream for given file and starting at given
   * offset and ending at endPosition - 1.
   * 
   * @see FileInputStream#FileInputStream(String)
   */
  public PartialFileInputStream(String fileName, long offset, long endPosition)
      throws IOException {
    super(fileName);
    initialize(offset, endPosition);
  }

  /**
   * Creates a new InputStream for given file and starting at given
   * offset/endPosition.
   * 
   * @see FileInputStream#FileInputStream(File)
   */
  public PartialFileInputStream(File file, long offset, long endPosition)
      throws IOException {
    super(file);
    initialize(offset, endPosition);
  }

  private void initialize(long offset, long endPosition) throws IOException {
    if (offset > 0) {
      super.getChannel().position(offset);
    }
    this.numBytesToRead = endPosition - offset;
    if (this.numBytesToRead < 0) {
      throw new IOException("invalid offset=" + offset + ", endPosition="
          + endPosition);
    }
  }

  /**
   * Reads a byte of data from this input stream. This method blocks if no input
   * is yet available.
   * 
   * @return the next byte of data, or <code>-1</code> if the end of the file is
   *         reached.
   * @exception IOException
   *              if an I/O error occurs.
   */
  @Override
  public int read() throws IOException {
    if (this.numBytesToRead > 0) {
      this.numBytesToRead--;
      return super.read();
    }
    return -1;
  }

  /**
   * Reads up to <code>b.length</code> bytes of data from this input stream into
   * an array of bytes. This method blocks until some input is available.
   * 
   * @param b
   *          the buffer into which the data is read.
   * @return the total number of bytes read into the buffer, or <code>-1</code>
   *         if there is no more data because the end of the file has been
   *         reached.
   * @exception IOException
   *              if an I/O error occurs.
   */
  @Override
  public int read(byte[] b) throws IOException {
    return this.read(b, 0, b.length);
  }

  /**
   * Reads up to <code>len</code> bytes of data from this input stream into an
   * array of bytes. If <code>len</code> is not zero, the method blocks until
   * some input is available; otherwise, no bytes are read and <code>0</code> is
   * returned.
   * 
   * @param b
   *          the buffer into which the data is read.
   * @param off
   *          the start offset in the destination array <code>b</code>
   * @param len
   *          the maximum number of bytes read.
   * @return the total number of bytes read into the buffer, or <code>-1</code>
   *         if there is no more data because the end of the file has been
   *         reached.
   * @exception NullPointerException
   *              If <code>b</code> is <code>null</code>.
   * @exception IndexOutOfBoundsException
   *              If <code>off</code> is negative, <code>len</code> is negative,
   *              or <code>len</code> is greater than
   *              <code>b.length - off</code>
   * @exception IOException
   *              if an I/O error occurs.
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (this.numBytesToRead > 0) {
      final int readLen;
      if (this.numBytesToRead >= len) {
        readLen = super.read(b, off, len);
      }
      else {
        readLen = super.read(b, off, (int)this.numBytesToRead);
      }
      if (readLen > 0) {
        this.numBytesToRead -= readLen;
      }
      return readLen;
    }
    return -1;
  }

  /**
   * Skips over and discards <code>n</code> bytes of data from the input stream.
   * 
   * <p>
   * The <code>skip</code> method may, for a variety of reasons, end up skipping
   * over some smaller number of bytes, possibly <code>0</code>. If
   * <code>n</code> is negative, an <code>IOException</code> is thrown, even
   * though the <code>skip</code> method of the {@link InputStream} superclass
   * does nothing in this case. The actual number of bytes skipped is returned.
   * 
   * <p>
   * This method may skip more bytes than are remaining in the backing file.
   * This produces no exception and the number of bytes skipped may include some
   * number of bytes that were beyond the EOF of the backing file. Attempting to
   * read from the stream after skipping past the end will result in -1
   * indicating the end of the file.
   * 
   * @param n
   *          the number of bytes to be skipped.
   * @return the actual number of bytes skipped.
   * @exception IOException
   *              if n is negative, if the stream does not support seek, or if
   *              an I/O error occurs.
   */
  @Override
  public long skip(long n) throws IOException {
    final long skippedLen;
    if (n >= this.numBytesToRead) {
      skippedLen = super.skip(n);
    }
    else {
      skippedLen = super.skip(this.numBytesToRead);
    }
    if (skippedLen > 0) {
      this.numBytesToRead -= skippedLen;
    }
    return skippedLen;
  }

  /**
   * Returns an estimate of the number of remaining bytes that can be read (or
   * skipped over) from this input stream without blocking by the next
   * invocation of a method for this input stream. The next invocation might be
   * the same thread or another thread. A single read or skip of this many bytes
   * will not block, but may read or skip fewer bytes.
   * 
   * <p>
   * In some cases, a non-blocking read (or skip) may appear to be blocked when
   * it is merely slow, for example when reading large files over slow networks.
   * 
   * @return an estimate of the number of remaining bytes that can be read (or
   *         skipped over) from this input stream without blocking.
   * @exception IOException
   *              if this file input stream has been closed by calling
   *              {@code close} or an I/O error occurs.
   */
  @Override
  public int available() throws IOException {
    final int avail = super.available();
    return avail <= this.numBytesToRead ? avail : (int)this.numBytesToRead;
  }
}
