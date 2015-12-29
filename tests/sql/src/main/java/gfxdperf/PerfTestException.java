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

package gfxdperf;

public class PerfTestException extends RuntimeException {

  public PerfTestException(String s) {
    super(s);
  }

  public PerfTestException(String s, Throwable t) {
    super(s,t);
  }

  /**
   * Returns the root cause of this exception, or null if the cause is unknown.
   */
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
}
