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

import hydra.*;

/**
 *  A class used to store keys for test configuration settings.
 */

public class OrderVectorPrms extends BasePrms {

  public static final int DEFAULT_SIZE = 1;

  /**
   *  (int)
   *  Size of each object.  Defaults to {@link #DEFAULT_SIZE}.
   */
  public static Long size;

  public static int getSize() {
    Long key = size;
    int val = tab().intAt( key, DEFAULT_SIZE );
    if ( val < 0 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
    return val;
  }

  static {
    setValues( OrderVectorPrms.class );
  }
  public static void main( String args[] ) {
    dumpKeys();
  }
}
