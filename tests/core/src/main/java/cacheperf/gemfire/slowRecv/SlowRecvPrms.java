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

package cacheperf.gemfire.slowRecv;

import hydra.*;

/**
 *
 *  A class used to set Prms for cacheperf.gemfire.SleepListener
 *
 */

public class SlowRecvPrms extends BasePrms {

  /**
   *  (boolean)
   *  Whether or not to expect async message queuing to occur.  
   *  We want to do validation that queuing occurred if true, but we 
   *  don't want to fail the test if we didn't expect queuing (for 
   *  our baseline).  (Defaults to false)
   */
  public static Long expectQueuing;
  public static boolean expectQueuing() {
    Long key = expectQueuing;
    return tasktab().booleanAt( key, tab().booleanAt( key, false ) );
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  static {
      setValues( SlowRecvPrms.class );
  }
  public static void main( String args[] ) {
      dumpKeys();
  }
}
