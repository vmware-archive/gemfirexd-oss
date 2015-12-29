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
 * Thrown when attempt to acquire a lock times out.
 * 
 * @author swale
 * @since 7.0
 */
public final class LockTimeoutException extends TimeoutException {

  private static final long serialVersionUID = -1269478414080740445L;

  /**
   * Constructs an instance of <code>LockTimeoutException</code> with the
   * specified detail message.
   * 
   * @param msg
   *          the detail message
   */
  public LockTimeoutException(String msg) {
    super(msg);
  }

  /**
   * Constructs an instance of <code>LockTimeoutException</code> with the
   * specified detail message and cause.
   * 
   * @param msg
   *          the detail message
   * @param cause
   *          the causal Throwable
   */
  public LockTimeoutException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructs an instance of <code>LockTimeoutException</code> with the
   * specified cause.
   * 
   * @param cause
   *          the causal Throwable
   * @since 6.5
   */
  public LockTimeoutException(Throwable cause) {
    super(cause);
  }
}
