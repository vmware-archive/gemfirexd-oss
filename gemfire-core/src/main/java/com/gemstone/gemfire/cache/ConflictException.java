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
 * Thrown when a transactional operation fails due to conflict with another
 * ongoing transaction. The conflict can be either write-write (for both
 * repeatable-read or read-committed) or read-write (for repeatable-read)
 * 
 * @author swale
 * @since 7.0
 */
public class ConflictException extends TransactionException {

  private static final long serialVersionUID = 7814350685723175624L;

  private boolean transactionSeverity = false;

  /**
   * Constructs an instance of <code>ConflictException</code> with the specified
   * detail message.
   * 
   * @param msg
   *          the detail message
   */
  public ConflictException(String msg) {
    super(msg);
  }

  /**
   * Constructs an instance of <code>ConflictException</code> with the specified
   * detail message and cause.
   * 
   * @param msg
   *          the detail message
   * @param cause
   *          the causal Throwable
   */
  public ConflictException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructs an instance of <code>ConflictException</code> with the specified
   * cause.
   * 
   * @param cause
   *          the causal Throwable
   * @since 6.5
   */
  public ConflictException(Throwable cause) {
    super(cause);
  }

  /**
   * Returns true if this exception causes the transaction to be aborted since
   * the transaction state can no longer be guaranteed to be consistent.
   */
  @Override
  public boolean isTransactionSeverity() {
    return this.transactionSeverity;
  }

  public void setTransactionSeverity() {
    this.transactionSeverity = true;
  }
}
