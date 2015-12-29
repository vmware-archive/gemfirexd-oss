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

import com.gemstone.gemfire.cache.CacheWriterException;

/**
 * An exception that is thrown by a {@link BridgeWriter} when a
 * problem occurs when communicating with a bridge server.
 *
 * @author David Whitlock
 * @since 3.5.2
 * @deprecated as of 5.7 use {@link com.gemstone.gemfire.cache.client pools} instead.
 */
@Deprecated
public class BridgeWriterException extends CacheWriterException {
private static final long serialVersionUID = -295001316745954159L;

  /**
   * Creates a new <code>BridgeWriterException</code> with the given
   * message. 
   */
  public BridgeWriterException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>BridgeWriterException</code> with the given
   * message and cause.
   */
  public BridgeWriterException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new <code>BridgeWriterException</code> with the given
   * cause.
   */
  public BridgeWriterException(Throwable cause) {
    super(cause);
  }

}
