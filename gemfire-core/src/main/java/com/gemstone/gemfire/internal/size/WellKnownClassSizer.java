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


/**
 * An efficient sizer for some commonly used
 * classes.
 * 
 * This will return 0 if it does not know
 * how to size the object
 * @author dsmith
 *
 */
public class WellKnownClassSizer {
  
  private static final int BYTE_ARRAY_OVERHEAD;
  private static final int STRING_OVERHEAD;
  
  static {
    try {
      ReflectionSingleObjectSizer objSizer = new ReflectionSingleObjectSizer();
      BYTE_ARRAY_OVERHEAD = (int) objSizer.sizeof(new byte[0], false);
      STRING_OVERHEAD = (int) (ReflectionSingleObjectSizer.sizeof(String.class) + objSizer.sizeof(new char[0], false));
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public static int sizeof(Object o) {
    int size = 0;
    
    if(o instanceof byte[]) {
      size =  BYTE_ARRAY_OVERHEAD + ((byte[]) o).length;
    }
    else if (o instanceof byte[][]) {
      final byte[][] byteArrays = (byte[][])o;
      final int len = byteArrays.length;
      byte[] bytes;
      size = (BYTE_ARRAY_OVERHEAD + 8) * len;
      for (int index = 0; index < byteArrays.length; index++) {
        bytes = byteArrays[index];
        if (bytes != null) {
          size += bytes.length;
        }
      }
    }
    else if(o instanceof String) {
      size = STRING_OVERHEAD + ((String) o).length() * 2; 
    } else {
      return 0;
    }
    
    size = (int) ReflectionSingleObjectSizer.roundUpSize(size);
    return size;
  }

}
