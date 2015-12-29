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
package com.gemstone.gemfire.internal.size;

public class SizeTestUtil {
  public static int OBJECT_SIZE=ReflectionSingleObjectSizer.OBJECT_SIZE;
  public static int REFERENCE_SIZE=ReflectionSingleObjectSizer.REFERENCE_SIZE;
  
  public static int roundup(int size) {
    //Rounds the size up to the next 8 byte boundary.
    return (int) (Math.ceil(size / 8.0) * 8);
  }
  
  private SizeTestUtil() {
    
  }

  
}
