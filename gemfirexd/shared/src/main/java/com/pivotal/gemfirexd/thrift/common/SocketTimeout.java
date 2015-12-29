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

import java.net.SocketException;

import com.gemstone.gemfire.internal.shared.SystemProperties;

/**
 * Interface that defines timeout and various keep-alive settings on the socket.
 * 
 * @author swale
 */
public interface SocketTimeout {

  /**
   * Get the socket read timeout.
   */
  public int getSoTimeout() throws SocketException;

  /**
   * Get the timeout set by previous setters calls and don't read from socket.
   */
  public int getRawTimeout();

  /**
   * Sets the socket read timeout.
   * 
   * @param timeout
   *          read timeout (SO_TIMEOUT) in milliseconds
   */
  public void setSoTimeout(int timeout) throws SocketException;

  /**
   * Sets the socket read timeout and keep-alive settings.
   * 
   * @param timeout
   *          read timeout (SO_TIMEOUT) in milliseconds
   * @param params
   *          Socket parameters including buffer sizes and keep-alive settings
   * @param props
   *          System properties instance to use for global settings
   */
  public void setTimeout(int timeout, SocketParameters params,
      SystemProperties props) throws SocketException;

  /** Close this socket */
  public void close();
}
