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
 * Thrown when a commit fails to complete due to errors
 * 
 * @author Mitch Thomas
 * @since 5.7
 * 
 * @deprecated as of 7.0, this exception is no longer thrown
 * 
 * @see ConflictException
 * @see TransactionInDoubtException
 */
@Deprecated
public class CommitIncompleteException extends TransactionException {
  private static final long serialVersionUID = 1017741483744420800L;

  @Deprecated
  public CommitIncompleteException(String message) {
    super(message);
  }
}
