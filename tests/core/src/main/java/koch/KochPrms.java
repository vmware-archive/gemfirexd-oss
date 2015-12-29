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

package koch;

import hydra.*;

/**
 *
 *  A class used to store keys for test configuration settings.
 *
 */

public class KochPrms extends hydra.BasePrms {

    /**
     *  Total number of objects to load and/or read per task.
     *  Not intended for use with oneof or range.
     */
    public static Long numObjsToDo;

    protected static int getNumObjsToDo() {
      Long key = numObjsToDo;
      int val = TestConfig.tab().intAt( key );
      if ( val >= 0 ) {
        return val;
      } else {
        throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
      }
    }

    /**
     *  Size of objects to use.
     */
    public static Long objSize;

    protected static int getObjSize() {
      Long key = objSize;
      int val = TestConfig.tab().intAt( key );
      if ( val >= 0 ) {
        return val;
      } else {
        throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
      }
    }

    /**
     *  Type of objects to use.
     */
    public static Long objType;

    protected static int getObjType() {
      Long key = objType;
      String val = TestConfig.tab().stringAt( key );
      if ( val.equalsIgnoreCase( "bytearray" ) ) {
        return BYTE_ARRAY;
      } else if ( val.equalsIgnoreCase( "string" ) ) {
        return STRING;
      } else {
        throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
      }
    }
    protected static final int BYTE_ARRAY = 0;
    protected static final int STRING     = 1;

    /**
     *  Whether to unload after an operation is carried out.
     *  Not intended for use with oneof or range.
     */
    public static Long unloadAfterOperation;

    protected static boolean unloadAfterOperation() {
      Long key = unloadAfterOperation;
      return TestConfig.tab().booleanAt( key, false );
    }

    static {
        setValues( KochPrms.class );
    }
    public static void main( String args[] ) {
        dumpKeys();
    }
}
