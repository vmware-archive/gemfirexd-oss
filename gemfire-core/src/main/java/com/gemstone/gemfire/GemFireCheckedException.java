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
   
package com.gemstone.gemfire;

import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * This is the abstract superclass of exceptions that are thrown and
 * declared.
 * <p>
 * This class ought to be called <em>GemFireException</em>, but that name
 * is reserved for an older class that extends {@link java.lang.RuntimeException}.
 * 
 * @see com.gemstone.gemfire.GemFireException
 * @since 5.1
 */
public abstract class GemFireCheckedException extends Exception {

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>GemFireException</code> with no detailed message.
   */
  public GemFireCheckedException() {
    super();
  }

  /**
   * Creates a new <code>GemFireCheckedException</code> with the given detail
   * message.
   */
  public GemFireCheckedException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>GemFireException</code> with the given detail
   * message and cause.
   */
  public GemFireCheckedException(String message, Throwable cause) {
    super(message);
    this.initCause(cause);
  }
  
  /**
   * Creates a new <code>GemFireCheckedException</code> with the given cause and
   * no detail message
   */
  public GemFireCheckedException(Throwable cause) {
    super();
    this.initCause(cause);
  }

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Returns the root cause of this <code>GemFireCheckedException</code> or
   * <code>null</code> if the cause is nonexistent or unknown.
   */
  public Throwable getRootCause() {
      if ( this.getCause() == null ) return null;
      Throwable root = this.getCause();
      while ( root != null ) {
//          if ( ! ( root instanceof GemFireCheckedException )) {
//              break;
//          }
//          GemFireCheckedException tmp = (GemFireCheckedException) root;
          if ( root.getCause() == null ) {
              break;
          } else {
              root = root.getCause();
          }
      }
      return root;
  }

  private transient DistributedMember origin;
  public DistributedMember getOrigin() {
    return this.origin;
  }
  public void setOrigin(DistributedMember origin) {
    this.origin = origin;
  }
}
