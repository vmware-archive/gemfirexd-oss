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
package com.gemstone.gemfire.internal.offheap;

/**
 * Represents a chunk of allocated memory that is not on the heap.
 * This interface provides methods that let you read and write to the chunk.
 * @author darrel
 *
 */
public interface MemoryChunk extends Releasable {
  
  /**
   * Returns the size of this memory chunk in bytes.
   */
  public int getSize();
  
  public byte readByte(int offset);
  public void writeByte(int offset, byte value);
  
  public void readBytes(int offset, byte[] bytes);
  public void writeBytes(int offset, byte[] bytes);
  public void readBytes(int offset, byte[] bytes, int bytesOffset, int size);
  public void writeBytes(int offset, byte[] bytes, int bytesOffset, int size);
  
  /**
   * Read the bytes in this range [src..src+size]
   * and write them to the range that starts at dst.
   * The number of bytes copied is size.
   */
  public void copyBytes(int src, int dst, int size);
}
