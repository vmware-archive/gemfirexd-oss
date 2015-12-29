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
 * Thrown when a commit fails due to a write conflict.
 * 
 * @author Darrel Schneider
 * 
 * @see CacheTransactionManager#commit
 * @since 4.0
 * 
 * @deprecated as of 7.0, this exception is no longer thrown (see
 *             {@link ConflictException} that is now thrown)
 */
@Deprecated
public class CommitConflictException extends ConflictException {

  private static final long serialVersionUID = -1491184174802596675L;

  /**
   * Constructs an instance of <code>CommitConflictException</code> with the
   * specified detail message.
   * 
   * @param msg
   *          the detail message
   */
  @Deprecated
  public CommitConflictException(String msg) {
    super(msg);
  }

  /**
   * Constructs an instance of <code>CommitConflictException</code> with the
   * specified detail message and cause.
   * 
   * @param msg
   *          the detail message
   * @param cause
   *          the causal Throwable
   */
  @Deprecated
  public CommitConflictException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructs an instance of <code>CommitConflictException</code> with the
   * specified cause.
   * 
   * @param cause
   *          the causal Throwable
   * @since 6.5
   */
  @Deprecated
  public CommitConflictException(Throwable cause) {
    super(cause);
  }

  /**
   * Returns true to indicate that this exception causes the transaction to be
   * aborted since the transaction state can no longer be guaranteed to be
   * consistent.
   */
  @Override
  public boolean isTransactionSeverity() {
    return true;
  }

  @Override
  public void setTransactionSeverity() {
  }
}
