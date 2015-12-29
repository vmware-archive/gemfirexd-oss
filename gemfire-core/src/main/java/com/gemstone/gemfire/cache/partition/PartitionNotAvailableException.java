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
package com.gemstone.gemfire.cache.partition;

import com.gemstone.gemfire.GemFireException;

/**
 * This exception is thrown when for the given fixed partition, datastore
 * (local-max-memory > 0) is not available.
 * 
 * @author kbachhhav
 * @since 6.6
 */

public class PartitionNotAvailableException extends GemFireException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new <code>PartitionNotAvailableException</code> with no detailed
   * message.
   */

  public PartitionNotAvailableException() {
    super();
  }

  /**
   * Creates a new <code>PartitionNotAvailableException</code> with the given
   * detail message.
   */
  public PartitionNotAvailableException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>PartitionNotAvailableException</code> with the given
   * cause and no detail message
   */
  public PartitionNotAvailableException(Throwable cause) {
    super(cause);
  }

  /**
   * Creates a new <code>PartitionNotAvailableException</code> with the given
   * detail message and cause.
   */
  public PartitionNotAvailableException(String message, Throwable cause) {
    super(message, cause);
  }

}
