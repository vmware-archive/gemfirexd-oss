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
package com.gemstone.gemfire.pdx.internal;

/**
 * This is a helper class that defines the size of the
 * primitive java types.
 * 
 * @author agingade
 * @since 6.6
 */
public class DataSize {

  private DataSize() {
    // no instances allowed
  }
  
  public static final int BYTE_SIZE = 1;
  
  public static final int BOOLEAN_SIZE = 1;
  
  public static final int CHAR_SIZE = 2;

  public static final int SHORT_SIZE = 2;
  
  public static final int INTEGER_SIZE = 4;
  
  public static final int FLOAT_SIZE = 4;
  
  public static final int LONG_SIZE = 8;
  
  public static final int DOUBLE_SIZE = 8;
  
  public static final int DATE_SIZE = 8;
}
