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
package com.gemstone.gemfire.internal.tcp;

import java.nio.ByteBuffer;

/**
 * You should only create an instance of this class if the bytes this buffer reads
 * will never change. If you want a buffer than can be refilled with other bytes then
 * create an instance of ByteBufferInputStream instead.
 * Note that even though this class is immutable the position on its ByteBuffer can change.
 * 
 * @author darrel
 * @since 6.6
 */
public class ImmutableByteBufferInputStream extends ByteBufferInputStream {

  /**
   * Create an immutable input stream by whose contents are the first length
   * bytes from the given input stream.
   * @param existing the input stream whose content will go into this stream. Note that this existing stream will be read by this class (a copy is not made) so it should not be changed externally.
   * @param length the number of bytes to put in this stream
   */
  public ImmutableByteBufferInputStream(ByteBufferInputStream existing,
      int length) {
    ByteBuffer bb = existing.slice();
    bb.limit(length);
    setBuffer(bb);
  }
  /**
   * Create an immutable input stream whose contents are the given bytes
   * @param bytes the content of this stream. Note that this byte array will be read by this class (a copy is not made) so it should not be changed externally.
   */
  public ImmutableByteBufferInputStream(byte[] bytes) {
    setBuffer(ByteBuffer.wrap(bytes));
  }

  /**
   * Create an immutable input stream whose contents are the given bytes
   * @param bb the content of this stream. Note that bb will be read by this class (a copy is not made) so it should not be changed externally.
   */
  public ImmutableByteBufferInputStream(ByteBuffer bb) {
    setBuffer(bb.slice());
  }
  /**
   * Create an immutable input stream by copying another. A somewhat shallow copy is made.
   * @param copy the input stream to copy. Note that this copy stream will be read by this class (a copy is not made) so it should not be changed externally.
   */
  public ImmutableByteBufferInputStream(ImmutableByteBufferInputStream copy) {
    super(copy);
  }
  public ImmutableByteBufferInputStream() {
    // for serialization
  }
  
  @Override
  public int available() {
    // If needed this could be implemented by computing size() - position()
    // but position() can change.
    throw new UnsupportedOperationException();
  }
  @Override
  public boolean markSupported() {
    return false;
  }
  @Override
  public void mark(int limit) {
    // unsupported but exception thrown by reset
  }
  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }
}
