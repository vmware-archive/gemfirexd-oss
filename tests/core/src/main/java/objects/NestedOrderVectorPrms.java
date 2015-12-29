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

public class NestedOrderVectorPrms extends BasePrms {

  public static final int DEFAULT_WIDTH = 2;
  public static final int DEFAULT_DEPTH = 2;

  /**
   *  (int)
   *  Width of each node in the object graph.  Defaults to {@link #DEFAULT_WIDTH}.
   */
  public static Long width;

  public static int getWidth() {
    Long key = width;
    int val = tab().intAt( key, DEFAULT_WIDTH );
    if ( val < 1 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
    return val;
  }

  /**
   *  (int)
   *  Depth of the object graph.  Defaults to {@link #DEFAULT_DEPTH}.
   */
  public static Long depth;

  public static int getDepth() {
    Long key = depth;
    int val = tab().intAt( key, DEFAULT_DEPTH );
    if ( val < 1 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
    return val;
  }

  static {
    setValues( NestedOrderVectorPrms.class );
  }
  public static void main( String args[] ) {
    dumpKeys();
  }
}
