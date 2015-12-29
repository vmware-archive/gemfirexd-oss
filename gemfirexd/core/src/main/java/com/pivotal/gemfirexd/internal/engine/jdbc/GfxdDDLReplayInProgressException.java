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

package com.pivotal.gemfirexd.internal.engine.jdbc;

import com.gemstone.gemfire.internal.cache.ForceReattemptException;

public class GfxdDDLReplayInProgressException extends ForceReattemptException {

  private static final long serialVersionUID = 776761114465958192L;

  // //////////////////// Constructors //////////////////////

  /**
   * Creates a new <code>GfxdDDLReplayInProgressException</code> with the given
   * detail message.
   */
  public GfxdDDLReplayInProgressException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>GfxdDDLReplayInProgressException</code> with the given
   * detail message and cause.
   */
  public GfxdDDLReplayInProgressException(String message, Throwable cause) {
    super(message, cause);
  }

  // ////////////////// Instance Methods ////////////////////

  /**
   * Returns the root cause of this
   * <code>GfxdDDLReplayInProgressException</code> or <code>null</code> if the
   * cause is nonexistent or unknown.
   */
  @Override
  public Throwable getRootCause() {
    if (this.getCause() == null) {
      return null;
    }
    Throwable root = this.getCause();
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return root;
  }

  @Override
  public String toString() {
    String result = super.toString();
    Throwable cause = getCause();
    if (cause != null) {
      String causeStr = cause.toString();
      final String glue = ", caused by ";
      StringBuilder sb = new StringBuilder(result.length() + causeStr.length()
          + glue.length());
      sb.append(result).append(glue).append(causeStr);
      result = sb.toString();
    }
    return result;
  }
}
