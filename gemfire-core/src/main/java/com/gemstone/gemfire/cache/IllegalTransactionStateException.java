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
 * Thrown when a transactional operation is attempted that the current
 * transactional state does not allow e.g. if commit when there is no active
 * transaction, or a write operation on a transaction that is in read-only mode
 * after a conflict.
 * 
 * @author swale
 * @since 7.0
 */
public class IllegalTransactionStateException extends TransactionException {

  private static final long serialVersionUID = -6116086438934563288L;

  /**
   * Constructs an instance of <code>IllegalTransactionStateException</code>
   * with the specified detail message.
   * 
   * @param msg
   *          the detail message
   */
  public IllegalTransactionStateException(String msg) {
    super(msg);
  }

  /**
   * Constructs an instance of <code>IllegalTransactionStateException</code>
   * with the specified detail message and cause.
   * 
   * @param msg
   *          the detail message
   * @param cause
   *          the causal Throwable
   */
  public IllegalTransactionStateException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
