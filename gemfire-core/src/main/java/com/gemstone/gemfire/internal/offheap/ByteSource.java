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
 * Interface acting as bridge to SqlFire layer.
 * Instances are off-heap values representing a row in gfxd table.
 * 
 * @author asif
 */
public interface ByteSource extends StoredObject {

  /**
   * If the data is serialized( ie. if it is a byte[][] or Delta) it returns
   * serialized bytes other with returns the byte[] ( which is the serialized
   * value)
   * 
   * @return byte[]
   */
  byte[] getRowBytes();

  public int getLength();

  public byte readByte(int offset);

  public void readBytes(int offset, byte[] bytes, int bytesOffset, int size);
}
