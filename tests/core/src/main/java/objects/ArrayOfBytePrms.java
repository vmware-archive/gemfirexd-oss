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

public class ArrayOfBytePrms extends BasePrms {

  public static final int DEFAULT_SIZE = 1024;
  public static final int MIN_SIZE = 0; // size of index
  public static final boolean DEFAULT_ENCODE_KEY = true;
  public static final boolean DEFAULT_ENCODE_TIMESTAMP = false;

  /**
   *  (int)
   *  Size of each object.  Defaults to {@link #DEFAULT_SIZE}.
   *  The minimum size is {@link #MIN_SIZE}.
   */
  public static Long size;

  public static int getSize() {
    Long key = size;
    int val = tab().intAt( key, DEFAULT_SIZE );
    if ( val < MIN_SIZE ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
    return val;
  }

  /**
   *  (boolean)
   *  Whether to encode the key into each object.  Defaults to
   *  {@link #DEFAULT_ENCODE_KEY}.
   */
  public static Long encodeKey;

  public static boolean encodeKey() {
    Long key = encodeKey;
    return tab().booleanAt( key, DEFAULT_ENCODE_KEY );
  }

  /**
   *  (boolean)
   *  Whether to encode a timestamp into each object.  Defaults to
   *  {@link #DEFAULT_ENCODE_TIMESTAMP}.
   */
  public static Long encodeTimestamp;

  public static boolean encodeTimestamp() {
    Long key = encodeTimestamp;
    boolean val = tab().booleanAt( key, DEFAULT_ENCODE_TIMESTAMP );
    if (val && !encodeKey()) {
      String s = "Must encode key to encode timestamp";
      throw new UnsupportedOperationException(s);
    }
    return val;
  }

  static {
    setValues( ArrayOfBytePrms.class );
  }
  public static void main( String args[] ) {
    dumpKeys();
  }
}
