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
package com.gemstone.gemfire.internal.memcached;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import com.gemstone.gemfire.internal.memcached.commands.ClientError;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer.Protocol;

/**
 * Reads the first line from the request and interprets the {@link Command}
 * from the memcached client
 * 
 * @author Swapnil Bawaskar
 *
 */
public class RequestReader {

  private static final Charset charsetASCII = Charset.forName("US-ASCII");
  
  private static final ThreadLocal<CharsetDecoder> asciiDecoder = new ThreadLocal<CharsetDecoder>() {
    @Override
    protected CharsetDecoder initialValue() {
      return charsetASCII.newDecoder();
    }
  };
  
  private ByteBuffer buffer;
  
  private ByteBuffer oneRequest;

  private ByteBuffer response;

  private static final int RESPONSE_HEADER_LENGTH = 24;

  private static final byte RESPONSE_MAGIC = (byte) 0x81;

  private static final int POSITION_OPCODE = 1;

  private static final int POSITION_OPAQUE = 12;

  private Socket socket;
  
  private final Protocol protocol;

  private CharBuffer commandBuffer = CharBuffer.allocate(11);   // no command exceeds 9 chars
  
  public RequestReader(Socket socket, Protocol protocol) {
    buffer = ByteBuffer.allocate(getBufferSize(socket.getChannel()));
    this.socket = socket;
    this.protocol = protocol;
  }

  public Command readCommand() throws IOException {
    if (protocol == Protocol.ASCII) {
      return readAsciiCommand();
    }
    return readBinaryCommand();
  }
  
  private Command readBinaryCommand() throws IOException {
    SocketChannel channel = this.socket.getChannel();
    if (channel == null || !channel.isOpen()) {
      throw new IllegalStateException("cannot read from channel");
    }
    if (oneRequest != null && oneRequest.hasRemaining()) {
      oneRequest = oneRequest.slice();
    } else {
      buffer.clear();
      channel.read(buffer);
      buffer.flip();
      oneRequest = buffer.duplicate();
    }
    Command cmd = Command.getBinaryCommand(oneRequest);
    if (ConnectionHandler.getLogger().fineEnabled()) {
      ConnectionHandler.getLogger().fine("read command "+cmd);
    }
    return cmd;
  }

  private Command readAsciiCommand() throws IOException {
    SocketChannel channel = this.socket.getChannel();
    if (channel == null || !channel.isOpen()) {
      throw new IllegalStateException("cannot read from channel");
    }
    buffer.clear();
    channel.read(buffer);
    buffer.flip();
    oneRequest = buffer.duplicate();
    return Command.valueOf(readCommand(oneRequest));
  }

  private String readCommand(ByteBuffer buffer) throws CharacterCodingException {
    commandBuffer.clear();
    asciiDecoder.get().decode(buffer, commandBuffer, false);
    commandBuffer.flip();
    return trimCommand(commandBuffer.toString()).toUpperCase();
  }
  
  private String trimCommand(String str) {
    int indexOfSpace = str.indexOf(' ');
    String retVal = str;
    if (indexOfSpace != -1) {
      retVal = str.substring(0, indexOfSpace);
    }
    int indexOfR = retVal.indexOf("\r");
    if (indexOfR != -1) {
      retVal = retVal.substring(0, indexOfR);
    }
    if (retVal.equals("")) {
      if (ConnectionHandler.getLogger().infoEnabled()) {
        // TODO i18n
        ConnectionHandler.getLogger().info("Unknown command. ensure client protocol is ASCII");
      }
      throw new IllegalArgumentException("Unknown command. ensure client protocol is ASCII");
    }
    return retVal;
  }
  
  private int getBufferSize(SocketChannel channel) {
    int size = 1024;
    try {
      size = channel.socket().getReceiveBufferSize();
    } catch (SocketException e) {
      // use default size
    }
    return size;
  }
  
  public ByteBuffer getRequest() {
    this.oneRequest.rewind();
    return this.oneRequest;
  }

  public ByteBuffer getResponse() {
    return getResponse(RESPONSE_HEADER_LENGTH);
  }

  /**
   * Returns an initialized byteBuffer for sending the reply
   * @param size size of ByteBuffer
   * @return the initialized response buffer
   */
  public ByteBuffer getResponse(int size) {
    if (this.response == null || this.response.capacity() < size) {
      this.response = ByteBuffer.allocate(size);
    }
    clear(this.response);
    this.response.put(RESPONSE_MAGIC);
    this.response.rewind();
    this.response.limit(size);
    return this.response;
  }

  private void clear(ByteBuffer response) {
    response.position(0);
    response.limit(response.capacity());
    response.put(getCleanByteArray());
    while (response.remaining() > getCleanByteArray().length) {
      response.put(getCleanByteArray());
    }
    while (response.remaining() > 0) {
      response.put((byte) 0);
    }
    response.clear();
  }

  private static byte[] cleanByteArray;
  
  private byte[] getCleanByteArray() {
    if (cleanByteArray != null) {
      return cleanByteArray;
    }
    cleanByteArray = new byte[RESPONSE_HEADER_LENGTH];
    for (int i=0; i<cleanByteArray.length; i++) {
      cleanByteArray[i] = 0;
    }
    return cleanByteArray;
  }

  public void sendReply(ByteBuffer reply) throws IOException {
    // for binary set the response opCode
    if (this.protocol == Protocol.BINARY) {
      reply.rewind();
      reply.put(POSITION_OPCODE, oneRequest.get(POSITION_OPCODE));
      reply.putInt(POSITION_OPAQUE, oneRequest.getInt(POSITION_OPAQUE));
      if (ConnectionHandler.getLogger().finerEnabled()) {
        ConnectionHandler.getLogger().finer("sending reply:"+reply+" "+Command.buffertoString(reply));
      }
    }
    SocketChannel channel = this.socket.getChannel();
    if (channel == null || !channel.isOpen()) {
      throw new IllegalStateException("cannot write to channel");
    }
    channel.write(reply);
  }

  public void sendException(Exception e) {
    SocketChannel channel = this.socket.getChannel();
    if (channel == null || !channel.isOpen()) {
      throw new IllegalStateException("cannot write to channel");
    }
    try {
      if (e instanceof ClientError) {
        channel.write(charsetASCII.encode(Reply.CLIENT_ERROR.toString()));
      } else {
        channel.write(charsetASCII.encode(Reply.ERROR.toString()));
      }
    } catch (IOException ex) {
    }
  }
}
