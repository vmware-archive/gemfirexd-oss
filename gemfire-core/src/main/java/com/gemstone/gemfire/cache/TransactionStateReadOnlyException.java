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
 * Thrown when a transactional operation is attempted after the current
 * transactional state does not allow a write or commit operation on a
 * transaction that is in read-only mode e.g. after a conflict, or if an illegal
 * operation was attempted (like on a region via remote function execution that
 * does not exist on the coordinator).
 * 
 * @author swale
 * @since 7.0
 */
public class TransactionStateReadOnlyException extends TransactionException {

  private static final long serialVersionUID = -6116086438934563288L;

  /**
   * Constructs an instance of <code>TransactionStateReadOnlyException</code>
   * with the specified detail message.
   * 
   * @param msg
   *          the detail message
   */
  public TransactionStateReadOnlyException(String msg) {
    super(msg);
  }

  /**
   * Constructs an instance of <code>TransactionStateReadOnlyException</code>
   * with the specified detail message and cause.
   * 
   * @param msg
   *          the detail message
   * @param cause
   *          the causal Throwable
   */
  public TransactionStateReadOnlyException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
