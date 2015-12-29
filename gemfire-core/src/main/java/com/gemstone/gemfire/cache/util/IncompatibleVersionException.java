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

import com.gemstone.gemfire.internal.shared.Version;

/**
 * An <code>Incompatible</code> indicates an unknown version.
 *
 * @author Barry Oglesby
 * @deprecated
 *
 * @since 5.6
 */
public class IncompatibleVersionException extends VersionException {

  private static final long serialVersionUID = 7008667865037538081L;

  /**
   * Constructs a new <code>IncompatibleVersionException</code>.
   *
   * @param clientVersion The client version
   * @param serverVersion The server version
   */
  public IncompatibleVersionException(Object clientVersion,
      Object serverVersion) {
    // the arguments should be of class Version, but that's an
    // internal class and this is an external class that shouldn't
    // ref internals in method signatures
    this("Client version " + clientVersion
        + " is incompatible with server version " + serverVersion);
  }

  /**
   * Constructs a new <code>IncompatibleVersionException</code>.
   *
   * @param message The exception message
   */
  public IncompatibleVersionException(String message) {
    super(message);
  }
}
