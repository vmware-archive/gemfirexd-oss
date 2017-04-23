/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package com.gemstone.gemfire.internal.cache.store;

import java.nio.ByteBuffer;

/**
 * Used for optimized serialization of ByteBuffer data by getting the
 * serialized form of data as a ByteBuffer directly.
 */
public abstract class SerializedBufferData {

  protected transient ByteBuffer serializedBuffer;

  public ByteBuffer getSerializedBuffer() {
    // duplicate the buffer so that caller will not be able to release
    // a direct ByteBuffer nor change the position etc
    final ByteBuffer buffer = this.serializedBuffer.duplicate();
    // rewind the buffer to the start to include serialization header
    buffer.rewind();
    return buffer;
  }
}
