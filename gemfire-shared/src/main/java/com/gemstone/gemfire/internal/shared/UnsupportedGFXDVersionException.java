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

package com.gemstone.gemfire.internal.shared;

/**
 * An <code>UnsupportedGFXDVersionException</code> indicates an unsupported
 * version.
 * 
 * @since gfxd 2.0
 */
public class UnsupportedGFXDVersionException extends Exception {

  private static final long serialVersionUID = 7664082377763265230L;

  /**
   * Constructs a new <code>UnsupportedGFXDVersionException</code>.
   * 
   * @param versionOrdinal
   *          The ordinal of the requested <code>Version</code>
   */
  public UnsupportedGFXDVersionException(short versionOrdinal) {
    super(String.valueOf(versionOrdinal));
  }

  /**
   * Constructs a new <code>UnsupportedGFXDVersionException</code>.
   * 
   * @param message
   *          The exception message
   */
  public UnsupportedGFXDVersionException(String message) {
    super(message);
  }
}
