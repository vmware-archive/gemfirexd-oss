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

package com.gemstone.gemfire.internal.util;

/**
 * The SunAPINotFoundException class is a RuntimeException indicating that the Sun API classes and components could
 * not be found, which is most likely the case when we are not running a Sun JVM (like HotSpot).
 * </p>
 * @author John Blum
 * @see java.lang.RuntimeException
 * @since 7.0
 */
@SuppressWarnings("unused")
public class SunAPINotFoundException extends RuntimeException {

  public SunAPINotFoundException() {
  }

  public SunAPINotFoundException(final String message) {
    super(message);
  }

  public SunAPINotFoundException(final Throwable t) {
    super(t);
  }

  public SunAPINotFoundException(final String message, final Throwable t) {
    super(message, t);
  }

}
