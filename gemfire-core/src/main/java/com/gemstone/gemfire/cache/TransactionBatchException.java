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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * Thrown when a batch of TX message apply fails. Contains the exception from
 * each member for a failed batch apply.
 * 
 * @author swale
 * @since 7.0
 */
public final class TransactionBatchException extends TransactionException {

  private static final long serialVersionUID = -6958193665347212112L;

  private final HashMap<DistributedMember, Throwable> exceptions =
    new HashMap<DistributedMember, Throwable>(4, 0.75f);

  public TransactionBatchException(String message, Throwable cause) {
    super(message, cause);
  }

  public Throwable getException(final DistributedMember member) {
    synchronized (this.exceptions) {
      return this.exceptions.get(member);
    }
  }

  public void addException(final DistributedMember member, final Throwable t) {
    synchronized (this.exceptions) {
      this.exceptions.put(member, t);
    }
  }

  /**
   * Throw the underlying cause {@link RuntimeException}/{@link Error} or self
   * if cause is not one of those. The motive is to throw same runtime
   * exceptions as would have been thrown with no batching.
   */
  public void throwException() throws TransactionException {
    final Throwable cause = getCause();
    if (cause instanceof ConflictException) {
      final ConflictException ce = (ConflictException)cause;
      ce.setTransactionSeverity();
      throw ce;
    }
    // check the map
    final List<Throwable> exceptions;
    synchronized (this.exceptions) {
      if (!this.exceptions.isEmpty()) {
        exceptions = new ArrayList<Throwable>(this.exceptions.values());
      }
      else {
        exceptions = Collections.emptyList();
      }
    }
    if (!exceptions.isEmpty()) {
      for (Throwable t : exceptions) {
        if (t instanceof ConflictException) {
          final ConflictException ce = (ConflictException)t;
          ce.setTransactionSeverity();
          throw ce;
        }
      }
    }
    if (cause instanceof TransactionException) {
      throw (TransactionException)cause;
    }
    // check the map
    if (!exceptions.isEmpty()) {
      for (Throwable t : exceptions) {
        if (t instanceof TransactionException) {
          throw (TransactionException)t;
        }
      }
    }
    if (cause instanceof RuntimeException) {
      throw (RuntimeException)cause;
    }
    if (cause instanceof Error) {
      throw (Error)cause;
    }
    throw this;
  }
}
