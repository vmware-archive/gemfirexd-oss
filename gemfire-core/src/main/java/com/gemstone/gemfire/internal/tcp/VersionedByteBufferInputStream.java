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

import com.gemstone.gemfire.internal.VersionedDataStream;
import com.gemstone.gemfire.internal.shared.Version;

/**
 * An extension to {@link ByteBufferInputStream} that implements
 * {@link VersionedDataStream} for a stream coming from a different product
 * version.
 * 
 * @author swale
 * @since 7.1
 */
public class VersionedByteBufferInputStream extends ByteBufferInputStream
    implements VersionedDataStream {

  private final Version version;

  /**
   * Create a ByteBuffer input stream whose contents are null at given product
   * {@link Version}.
   * 
   * @param version
   *          the product version for which this stream was created
   */
  public VersionedByteBufferInputStream(Version version) {
    super();
    this.version = version;
  }

  /**
   * Create a ByteBuffer input stream whose contents are the given
   * {@link ByteBuffer} at given product {@link Version}.
   * 
   * @param buffer
   *          the byte buffer to read
   * @param version
   *          the product version for which this stream was created
   */
  public VersionedByteBufferInputStream(ByteBuffer buffer, Version version) {
    super(buffer);
    this.version = version;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Version getVersion() {
    return this.version;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return super.toString() + " (" + this.version + ')';
  }
}
