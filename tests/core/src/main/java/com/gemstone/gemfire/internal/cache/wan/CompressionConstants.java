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
package com.gemstone.gemfire.internal.cache.wan;

public interface CompressionConstants {

  /** Constants for 6-bit code values. */

  /** No operation: used to pad words on flush. */
  static final int NOP     = 0;  

  /** Introduces raw byte format. */
  static final int RAW     = 1;  

  /** Format indicator for characters found in lookup table. */
  static final int BASE    = 2;  

  /** A character's code is it's index in the lookup table. */
  static final String codeTable =
      "abcdefghijklmnopqrstuvwxyz" +
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ ,.!?\"'()";
  
}
