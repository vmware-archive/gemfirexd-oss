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

package objects;

import java.io.Serializable;

/**
 *  An integer based on index
 */

public class TestInteger implements Serializable {

  public static Integer init( int index ) {
    return new Integer( index );
  }

  public static int getIndex( Integer obj ) {
    return obj.intValue();
  }

  public static void validate( int index, Integer obj) {
    int encodedIndex = getIndex( obj );
    if ( encodedIndex != index ) {
      throw new ObjectValidationException( "Expected index " + index + ", got " + encodedIndex );
    }
  }
}
