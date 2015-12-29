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
package com.gemstone.gemfire.cache.client.internal;

/**
 * An operation to perform on a server. Used by
 * {@link ExecutablePool} to attempt the operation on 
 * multiple servers until the retryAttempts is exceeded.
 * @author dsmith
 * @since 5.7
 *
 */
public interface Op {

  /**
   * Attempts to execute this operation by sending its message out on the
   * given connection, waiting for a response, and returning it.
   * @param cnx the connection to use when attempting execution of the operation.
   * @return the result of the operation
   *         or <code>null</code if the operation has no result.
   * @throws Exception if the execute failed
   */
  Object attempt(Connection cnx) throws Exception;

  /**
   * @return true if this Op should use a threadLocalConnection, false otherwise
   */
  boolean useThreadLocalConnection();
}