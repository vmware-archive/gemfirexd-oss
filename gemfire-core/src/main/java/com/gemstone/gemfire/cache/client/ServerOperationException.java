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
package com.gemstone.gemfire.cache.client;

/**
 * An exception indicating that a failure has happened on the server
 * while processing an operation that was sent to it by a client.
 * @author darrel
 * @since 5.7
 */
public class ServerOperationException extends ServerConnectivityException {
private static final long serialVersionUID = -3106323103325266219L;

  /**
   * Create a new instance of ServerOperationException without a detail message or cause.
   */
  public ServerOperationException() {
  }

  /**
   * 
   * Create a new instance of ServerOperationException with a detail message
   * @param message the detail message
   */
  public ServerOperationException(String message) {
    super(message);
  }

  /**
   * Create a new instance of ServerOperationException with a detail message and cause
   * @param message the detail message
   * @param cause the cause
   */
  public ServerOperationException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Create a new instance of ServerOperationException with a cause
   * @param cause the cause
   */
  public ServerOperationException(Throwable cause) {
    super(cause);
  }

}
