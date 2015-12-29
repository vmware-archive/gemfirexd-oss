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

package com.gemstone.gemfire.cache.query;

import com.gemstone.gemfire.cache.CacheRuntimeException;
/**
 * Thrown when the query execution takes more than the specified max time.
 * The Max query execution time is set using the system  variable 
 * gemfire.Cache.MAX_QUERY_EXECUTION_TIME. 
 *
 * @author agingade
 * @since 6.0
 */
public class QueryExecutionTimeoutException extends CacheRuntimeException {
  
  /**
   * Creates a new instance of <code>QueryExecutionTimeoutException</code> without detail message.
   */
  public QueryExecutionTimeoutException() {
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionTimeoutException</code> with the specified detail message.
   * @param msg the detail message.
   */
  public QueryExecutionTimeoutException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionTimeoutException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public QueryExecutionTimeoutException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionTimeoutException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public QueryExecutionTimeoutException(Throwable cause) {
    super(cause);
  }
}
