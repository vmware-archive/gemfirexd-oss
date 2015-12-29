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

package com.pivotal.gemfirexd.thrift.common;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.internal.shared.SystemProperties;
import com.pivotal.gemfirexd.thrift.HostAddress;

/**
 * A custom SSL TSocket allowing to increase input/output buffer sizes.
 * 
 * Currently this uses blocking Sockets and not NIO. Using SSLEngine is tricky
 * to get correct so will be implemented later. When it shall implement it, then
 * this will extend {@link GfxdTSocket} so higher layer should use the
 * "instanceof {@link GfxdTSocket}" check to determine whether to use selector
 * based server or old threaded one.
 * 
 * @author swale
 * @since gfxd 1.1
 */
public final class GfxdTSSLSocket extends TSocket implements SocketTimeout {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(GfxdTSSLSocket.class.getName());

  /**
   * Resolved remote host address
   */
  private InetAddress hostAddress;

  /**
   * Remote port
   */
  private int port;

  /**
   * Socket timeout
   */
  private volatile int timeout;

  private int inputBufferSize = SystemProperties.getClientInstance()
      .getSocketInputBufferSize();
  private int outputBufferSize = SystemProperties.getClientInstance()
      .getSocketOutputBufferSize();

  /**
   * Constructor that takes an already created socket.
   * 
   * @param socket
   *          Already created socket object
   * 
   * @throws TTransportException
   *           if there is an error setting up the streams
   */
  public GfxdTSSLSocket(Socket socket, int timeout, SocketParameters params,
      SystemProperties props) throws TTransportException {
    super(socket);

    if (isOpen()) {
      try {
        setProperties(socket, timeout, params, props);
        this.inputStream_ = new BufferedInputStream(socket.getInputStream(),
            this.inputBufferSize);
        this.outputStream_ = new BufferedOutputStream(socket.getOutputStream(),
            this.outputBufferSize);
      } catch (IOException ioe) {
        close();
        throw new TTransportException(TTransportException.NOT_OPEN, ioe);
      }
    }
  }

  /**
   * Creates a new SSL socket that will connect to the given host on the given
   * port.
   * 
   * @param host
   *          Remote HostAddress including port
   * @param params
   *          Socket parameters including SSL properties
   * @param props
   *          the system properties instance to use and initialize global socket
   *          options like keepalive and buffer sizes that are not set in params
   */
  public GfxdTSSLSocket(HostAddress host, SocketParameters params,
      SystemProperties props) throws TTransportException {
    this(host.resolveHost(), host.getPort(), params, props, params
        .getReadTimeout(0));
  }

  /**
   * Creates a new unconnected SSL socket that will connect to the given host on
   * the given port.
   * 
   * @param hostAddress
   *          Resolved remote host address
   * @param port
   *          Remote port
   * @param params
   *          Socket parameters including SSL properties
   * @param props
   *          the system properties instance to use and initialize global socket
   *          options like keepalive and buffer sizes that are not set in params
   * @param timeout
   *          Socket timeout
   */
  public GfxdTSSLSocket(InetAddress hostAddress, int port,
      SocketParameters params, SystemProperties props, int timeout)
      throws TTransportException {
    super(initSSLSocket(hostAddress, port, params, timeout));
    this.hostAddress = hostAddress;
    this.port = port;

    setProperties(getSocket(), timeout, params, props);

    if (!isOpen()) {
      this.open();
    }
  }

  /**
   * Initializes and connects the SSL socket object
   */
  private static Socket initSSLSocket(InetAddress hostAddress, int port,
      SocketParameters sockParams, int timeout) throws TTransportException {
    final Socket socket = GfxdTSSLSocketFactory.getClientSocket(hostAddress,
        port, timeout, sockParams);
    return socket;
  }

  /**
   * Sets the socket read timeout.
   * 
   * @param timeout
   *          read timeout (SO_TIMEOUT) in milliseconds
   */
  @Override
  public void setTimeout(int timeout) {
    try {
      getSocket().setSoTimeout(timeout);
      this.timeout = timeout;
    } catch (SocketException se) {
      LOGGER.warn("Could not set socket timeout.", se);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getSoTimeout() throws SocketException {
    return getSocket().getSoTimeout();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getRawTimeout() {
    return this.timeout;
  }

  /**
   * Sets the socket read timeout.
   * 
   * @param timeout
   *          read timeout (SO_TIMEOUT) in milliseconds
   */
  @Override
  public void setSoTimeout(int timeout) throws SocketException {
    getSocket().setSoTimeout(timeout);
    this.timeout = timeout;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setTimeout(int timeout, SocketParameters params,
      SystemProperties props) throws SocketException {
    this.timeout = GfxdTSocket.setTimeout(getSocket(), timeout, params, props);
  }

  /**
   * Sets the socket properties like timeout, keepalive, buffer sizes.
   * 
   * @param timeout
   *          Milliseconds timeout
   * @param params
   *          Socket parameters including buffer sizes and keep-alive settings
   * @param props
   *          the system properties instance to use and initialize global socket
   *          options like keepalive and buffer sizes that are not set in params
   */
  protected void setProperties(Socket socket, int timeout,
      SocketParameters params, SystemProperties props)
      throws TTransportException {
    this.inputBufferSize = params.getInputBufferSize(props
        .getSocketInputBufferSize());
    this.outputBufferSize = params.getOutputBufferSize(props
        .getSocketOutputBufferSize());
    try {
      socket.setSoLinger(false, 0);
      socket.setTcpNoDelay(true);
      this.timeout = GfxdTSocket.setTimeout(socket, timeout, params, props);
    } catch (SocketException se) {
      LOGGER.warn("Could not set socket timeout.", se);
      throw new TTransportException(TTransportException.NOT_OPEN,
          "Could not set socket timeout.", se);
    }
  }

  /**
   * Connects the socket, creating a new socket object if necessary.
   */
  @Override
  public void open() throws TTransportException {
    if (isOpen()) {
      throw new TTransportException(TTransportException.ALREADY_OPEN,
          "Socket already connected.");
    }

    if (this.hostAddress == null) {
      throw new TTransportException(TTransportException.NOT_OPEN,
          "Cannot open null host.");
    }
    if (this.port <= 0) {
      throw new TTransportException(TTransportException.NOT_OPEN,
          "Cannot open without port.");
    }

    final Socket socket = getSocket();
    try {
      socket.connect(new InetSocketAddress(this.hostAddress, this.port),
          this.timeout);
      this.inputStream_ = new BufferedInputStream(socket.getInputStream(),
          this.inputBufferSize);
      this.outputStream_ = new BufferedOutputStream(socket.getOutputStream(),
          this.outputBufferSize);
    } catch (IOException ioe) {
      close();
      throw new TTransportException(TTransportException.NOT_OPEN, ioe);
    }
  }
}
