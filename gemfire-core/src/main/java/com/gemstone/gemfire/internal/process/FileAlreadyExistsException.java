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
package com.gemstone.gemfire.internal.process;

/**
 * A FileAlreadyExistsException is thrown when a pid file already exists
 * and the launcher expects to create a new pid file without forcing the
 * deletion of the old one.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public final class FileAlreadyExistsException extends Exception {
  private static final long serialVersionUID = 5471082555536094256L;

  /**
   * Creates a new <code>FileAlreadyExistsException</code>.
   */
  public FileAlreadyExistsException(final String message) {
    super(message);
  }

  /**
   * Creates a new <code>FileAlreadyExistsException</code> that was
   * caused by a given exception
   */
  public FileAlreadyExistsException(final String message, final Throwable thr) {
    super(message, thr);
  }

  /**
   * Creates a new <code>FileAlreadyExistsException</code> that was
   * caused by a given exception
   */
  public FileAlreadyExistsException(final Throwable thr) {
    super(thr.getMessage(), thr);
  }
}
