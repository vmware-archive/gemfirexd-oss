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

package com.gemstone.gemfire.cache;

/**
 * An exception thrown by a <code>Gateway</code>.
 *
 * @author Barry Oglesby
 *
 * @since 4.2
 */
public class GatewayException extends OperationAbortedException {
private static final long serialVersionUID = 8090143153569084886L;

  /**
   * Constructor.
   * Creates a new instance of <code>GatewayException</code>.
   */
  public GatewayException() {
  }

  /**
   * Constructor.
   * Creates an instance of <code>GatewayException</code> with the
   * specified detail message.
   * @param msg the detail message
   */
  public GatewayException(String msg) {
    super(msg);
  }

  /**
   * Constructor.
   * Creates an instance of <code>GatewayException</code> with the
   * specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public GatewayException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructor.
   * Creates an instance of <code>GatewayException</code> with the
   * specified cause.
   * @param cause the causal Throwable
   */
  public GatewayException(Throwable cause) {
    super(cause);
  }
}
