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
package com.pivotal.gemfirexd.internal.engine.distributed;

/**
 * This is a helper class which sets the appropriate bit on for a column in the
 * tuple if there is data in it. Each bit will represent one column.
 * 
 * @author Asif
 * @since GemFireXD
 */
public abstract class ActiveColumnBits {

  private static final byte[] ON_COLUMN_FLAG = new byte[] { 0x01, 0x02, 0x04,
      0x08, 0x10, 0x20, 0x40, (byte)0x80 };

  public static byte setFlagForNormalizedColumnPosition(int normalizedColPos,
      boolean isOn, byte flag) {
    return (isOn ? setFlagForNormalizedColumnPosition(normalizedColPos, flag)
        : clearFlagForNormalizedColumnPosition(normalizedColPos, flag));
  }

  public static byte setFlagForNormalizedColumnPosition(int normalizedColPos,
      byte flag) {
    return (byte)(flag | ON_COLUMN_FLAG[normalizedColPos]);
  }

  public static byte clearFlagForNormalizedColumnPosition(int normalizedColPos,
      byte flag) {
    return (byte)(flag & ~ON_COLUMN_FLAG[normalizedColPos]);
  }

  public static boolean isNormalizedColumnOn(int normalizedColPos, byte flag) {
    return (flag & ON_COLUMN_FLAG[normalizedColPos]) != 0;
  }
}
