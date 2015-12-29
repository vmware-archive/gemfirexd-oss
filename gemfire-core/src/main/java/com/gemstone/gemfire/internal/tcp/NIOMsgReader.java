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

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.gemstone.gemfire.internal.shared.Version;

/**
 * A message reader which reads from the socket
 * using (blocking) nio.
 * @author dsmith
 *
 */
public class NIOMsgReader extends MsgReader {
  
  /** the buffer used for NIO message receipt */
  private ByteBuffer nioInputBuffer;
  private final SocketChannel inputChannel;
  private int lastReadPosition;
  private int lastProcessedPosition; 
  
  public NIOMsgReader(Connection conn, Version version) throws SocketException {
    super(conn, version);
    this.inputChannel = conn.getSocket().getChannel();
  }


  @Override
  public ByteBuffer readAtLeast(int bytes) throws IOException {
    ensureCapacity(bytes);
    
    while(lastReadPosition - lastProcessedPosition < bytes) {
      nioInputBuffer.limit(nioInputBuffer.capacity());
      nioInputBuffer.position(lastReadPosition);
      int bytesRead = inputChannel.read(nioInputBuffer);
      if(bytesRead < 0) {
        throw new EOFException();
      }
      lastReadPosition = nioInputBuffer.position();
    }
    nioInputBuffer.limit(lastProcessedPosition + bytes);
    nioInputBuffer.position(lastProcessedPosition);
    lastProcessedPosition = nioInputBuffer.limit();

    return nioInputBuffer;
  }
  
  /** gets the buffer for receiving message length bytes */
  protected void ensureCapacity(int bufferSize) {
    //Ok, so we have a buffer that's big enough
    if(nioInputBuffer != null && nioInputBuffer.capacity() > bufferSize) {
      if(nioInputBuffer.capacity() - lastProcessedPosition < bufferSize) {
        nioInputBuffer.limit(lastReadPosition);
        nioInputBuffer.position(lastProcessedPosition);
        nioInputBuffer.compact();
        lastReadPosition = nioInputBuffer.position();
        lastProcessedPosition = 0;
      }
      return;
    }
    
    //otherwise, we have no buffer to a buffer that's too small
    
    if (nioInputBuffer == null) {
      int allocSize = conn.getReceiveBufferSize();
      if (allocSize == -1) {
        allocSize = conn.owner.getConduit().tcpBufferSize;
      }
      if(allocSize > bufferSize) {
        bufferSize = allocSize;
      }
    }
    ByteBuffer oldBuffer = nioInputBuffer;
    nioInputBuffer = Buffers.acquireReceiveBuffer(bufferSize, getStats());
    
    if(oldBuffer != null) {
      oldBuffer.limit(lastReadPosition);
      oldBuffer.position(lastProcessedPosition);
      nioInputBuffer.put(oldBuffer);
      lastReadPosition = nioInputBuffer.position(); // fix for 45064
      lastProcessedPosition = 0;
      Buffers.releaseReceiveBuffer(oldBuffer, getStats());
    }
  }

  @Override
  public void close() {
    ByteBuffer tmp = this.nioInputBuffer;
    if(tmp != null) {
      this.nioInputBuffer = null;
      Buffers.releaseReceiveBuffer(tmp, getStats());
    }
  }
}
