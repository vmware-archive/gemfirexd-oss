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
package com.gemstone.gemfire.distributed;

import java.util.concurrent.Future;
import com.gemstone.gemfire.CancelException;

/**
 * Thrown when a {@link Future} has been cancelled.
 * 
 * @since 6.0
 */

public class FutureCancelledException extends CancelException {
  private static final long serialVersionUID = -4599338440381989844L;

  public FutureCancelledException() {
    super();
  }

  public FutureCancelledException(String message, Throwable cause) {
    super(message, cause);
  }

  public FutureCancelledException(Throwable cause) {
    super(cause);
  }

  public FutureCancelledException(String s) {
    super(s);
  }

}
