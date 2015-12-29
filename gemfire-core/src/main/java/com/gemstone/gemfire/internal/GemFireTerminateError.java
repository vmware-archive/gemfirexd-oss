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

package com.gemstone.gemfire.internal;

/**
 * Internal error thrown to indicate that the current command-line utility must
 * be terminated. Utility methods invoked from command-line must throw this to
 * indicate fatal conditions instead of invoking {@link System#exit(int)} to
 * enable using those from within other contexts like hydra tests.
 * 
 * @author swale
 * @since 7.0
 */
public class GemFireTerminateError extends Error {

  private static final long serialVersionUID = -354055516859970116L;

  private int exitCode;

  public GemFireTerminateError() {
  }

  public GemFireTerminateError(String message, int exitCode) {
    super(message);
    this.exitCode = exitCode;
  }

  public GemFireTerminateError(String message, int exitCode, Throwable cause) {
    super(message, cause);
    this.exitCode = exitCode;
  }

  public int getExitCode() {
    return this.exitCode;
  }
}
