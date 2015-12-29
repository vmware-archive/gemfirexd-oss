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

import com.gemstone.gemfire.internal.shared.ClientSharedUtils;

/**
 * Provides hexadecimal conversion and display utilities.
 * 
 * @author bakera
 */
public abstract class Hex {

  private Hex() {
    // no instances
  }

  /**
   * Converts the byte array to a hex string.
   * @param buf the buffer to convert
   * @return the hex string
   */
  public static String toHex(byte[] buf) {
    return ClientSharedUtils.toHexString(buf, 0, buf.length);
  }

  /**
   * Converts the byte array subset to a hex string.
   * @param buf the buffer to convert
   * @param offset the offset
   * @param length the length
   * @return the hex string
   */
  public static String toHex(byte[] buf, int offset, int length) {
    if (buf != null) {
      return ClientSharedUtils.toHexString(buf, offset, length);
    }
    else {
      return null;
    }
  }
}
