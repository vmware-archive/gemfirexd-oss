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

package com.gemstone.gemfire.internal.shared;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * @author swale
 * @since gfxd 1.1
 */
public final class ChannelBufferFramedOutputStream extends
    ChannelBufferOutputStream {

  protected boolean doWriteFrameSize;

  public ChannelBufferFramedOutputStream(WritableByteChannel channel)
      throws IOException {
    super(channel);
    // position the buffer to skip the length of frame at the start
    this.buffer.position(4);
    this.doWriteFrameSize = true;
  }

  public ChannelBufferFramedOutputStream(WritableByteChannel channel,
      int bufferSize) throws IOException {
    super(channel, bufferSize);
    // position the buffer to skip the length of frame at the start
    this.buffer.position(4);
    this.doWriteFrameSize = true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() throws IOException {
    final ByteBuffer buffer = this.buffer;
    if (this.doWriteFrameSize) {
      // write the length now at the start
      buffer.putInt(0, buffer.position() - 4);
      super.flushBufferBlocking(buffer);
    }
    else {
      if (buffer.position() > 0) {
        super.flushBufferBlocking(buffer);
      }
      // reset "doWriteFrameSize" to true to indicate start of a new frame
      // whose frame size will be written at next flush
      this.doWriteFrameSize = true;
    }
    // position the buffer to skip the length of frame at the start
    buffer.position(4);
  }

  @Override
  protected void flushBufferBlocking(final ByteBuffer buffer)
      throws IOException {
    if (this.doWriteFrameSize) {
      // write the length now at the start
      buffer.putInt(0, buffer.position() - 4);
      // set the flag to false to indicate that frame size no longer
      // needs to be written till the actual end of message (which will
      // happen when the explicit flush is invoked)
      this.doWriteFrameSize = false;
    }
    super.flushBufferBlocking(buffer);
  }
}
