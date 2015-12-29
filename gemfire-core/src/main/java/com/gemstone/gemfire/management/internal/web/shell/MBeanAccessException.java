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
package com.gemstone.gemfire.management.internal.web.shell;

/**
 * The MBeanAccessException class is a RuntimeException indicating that an attempt to access an MBean attribute or
 * invocation of an MBean operation failed.
 * <p/>
 * @author John Blum
 * @see java.lang.RuntimeException
 * @since 7.5
 */
@SuppressWarnings("unused")
public class MBeanAccessException extends RuntimeException {

  public MBeanAccessException() {
  }

  public MBeanAccessException(final String message) {
    super(message);
  }

  public MBeanAccessException(final Throwable cause) {
    super(cause);
  }

  public MBeanAccessException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
