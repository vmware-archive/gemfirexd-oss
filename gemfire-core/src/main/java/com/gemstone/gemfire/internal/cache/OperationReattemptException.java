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

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.CancelException;

/**
 * Currently thrown by GemFireXD layer to indicate an operation retry to GemFire
 * layer.
 * 
 * @author sjigyasu
 * 
 */
public class OperationReattemptException extends CancelException {

  private static final long serialVersionUID = -3273669322279041732L;

  /**
   * Constructs a new <code>OperationReattemptException</code>.
   */
  public OperationReattemptException() {
    super();
  }

  /**
   * Constructs a new <code>OperationReattemptException</code> with a message
   * string.
   * 
   * @param msg
   *          a message string
   */
  public OperationReattemptException(String msg) {
    super(msg);
  }

  /**
   * Constructs a new <code>OperationReattemptException</code> with a message
   * string and a cause.
   * 
   * @param msg
   *          the message string
   * @param cause
   *          a causal Throwable
   */
  public OperationReattemptException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
