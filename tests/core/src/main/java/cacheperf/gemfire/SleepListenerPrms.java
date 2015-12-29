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

package cacheperf.gemfire;

import hydra.*;

/**
 *
 *  A class used to set Prms for cacheperf.gemfire.SleepListener
 *
 */

public class SleepListenerPrms extends BasePrms {

  /**
   *  (int)
   *  The time to sleep in milliseconds in the SleepListener
   *  Defaults to 0 (no sleep).  Suitable for oneof, range, etc.
   */
  public static Long sleepMs;
  public static int getSleepMs() {
    Long key = sleepMs;
    int val = tasktab().intAt( key, tab().intAt( key, 0 ) );
    if ( val < 0 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ":  " + val );
    }
    return val;
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  static {
      setValues( SleepListenerPrms.class );
  }
  public static void main( String args[] ) {
      dumpKeys();
  }
}
