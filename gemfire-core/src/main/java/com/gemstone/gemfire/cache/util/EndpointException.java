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
package com.gemstone.gemfire.cache.util;

/**
 * An <code>EndpointException</code> is a generic exception that indicates
 * a client <code>Endpoint</code> exception has occurred. All other
 * <code>Endpoint</code> exceptions are subclasses of this class. Since
 * this class is abstract, only subclasses are instantiated.
 *
 * @author Barry Oglesby
 *
 * @since 5.0.2
 * @deprecated as of 5.7 use {@link com.gemstone.gemfire.cache.client pools} instead.
 */
@Deprecated
public abstract class EndpointException extends Exception {
  
  /** Constructs a new <code>EndpointException</code>. */
  public EndpointException() {
    super();
  }

  /** Constructs a new <code>EndpointException</code> with a message string. */
  public EndpointException(String s) {
    super(s);
  }

  /** Constructs a <code>EndpointException</code> with a message string and
   * a base exception
   */
  public EndpointException(String s, Throwable cause) {
    super(s, cause);
  }

  /** Constructs a <code>EndpointException</code> with a cause */
  public EndpointException(Throwable cause) {
    super(cause);
  }

  @Override
  public String toString() {
    String result = super.toString();
    Throwable cause = getCause();
    if (cause != null) {
      String causeStr = cause.toString();
      final String glue = ", caused by ";
      StringBuffer sb = new StringBuffer(result.length() + causeStr.length() + glue.length());
      sb.append(result)
        .append(glue)
        .append(causeStr);
      result = sb.toString();
    }
    return result;
  }
}
