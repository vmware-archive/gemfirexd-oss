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
package util;

import hydra.*;

/**
 *
 *  A class used to store keys for test configuration settings.
 *
 */

/**
 *
 *  A class used to store keys for test configuration settings.
 *  @author jfarris
 *  @since 5.0
 *
 */


public class CpuLoadPrms extends BasePrms {

  /**
   *  (int)
   *  The time to sleep in milliseconds while doing non-GemFire work
   *  Defaults to 0 (no sleep).  Suitable for oneof, range, etc.
   */
  public static Long sleepMsNonGemFire;
  public static int getSleepMsNonGemFire() {
    Long key = sleepMsNonGemFire;
    int val = tasktab().intAt( key, tab().intAt( key, 0 ) );
    if ( val < 0 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
    return val;
  }

 /**
   *  (boolean)
   *  Whether test should include non-GemFire work task(s)
   *  Not for use with oneof or range.pr
   */
  public static Long doWorkTask;
  public static boolean doWorkTask() {
    Long key = doWorkTask;
    return tasktab().booleanAt( key, tab().booleanAt( key, true ) );
  }

 /**
   *  (int)
   *  The  required CPU load (mean CPU Active) for task doing non-GemFire work
   *  Defaults to 0 
   */
  public static Long workLoad;
  public static int  getWorkLoad() {
    Long key = workLoad;
    int val = tasktab().intAt( key, tab().intAt( key, 0 ) );
    if ( val < 0 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
    return val;
  }

/**
   *  (int)
   *  The  minimum required CPU load (mean CPU Active) for non-GemFire work
   *  Defaults to 0 
   */
  public static Long cpuLoadMin;
  public static int  getCpuLoadMin() {
    Long key = cpuLoadMin; 
    int val = tasktab().intAt( key, tab().intAt( key, 0 ) );
    if ( val < 0 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
    return val;
  }

  /**
   *  (int)
   *  The  maximum required CPU load (mean CPU Active) for non-GemFire work
   *  Defaults to 0 
   */
  public static Long cpuLoadMax;
  public static int  getCpuLoadMax() {
    Long key = cpuLoadMax;
    int val = tasktab().intAt( key, tab().intAt( key, 0 ) );
    if ( val < 0 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
    return val;
  }

  /**
   *  (int)
   *  The duration (in seconds) of a "do work" task.
   *  Defaults to 0
   */
  public static Long workSec;
  public static int getWorkSec() {
    Long key = workSec;
    int val = tasktab().intAt( key, tab().intAt( key, 0 ) );
    if ( val < 0 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
    return val;
  }


  //----------------------------------------------------------------------------
  //  Required stuff
  //----------------------------------------------------------------------------

  static {
    setValues( CpuLoadPrms.class );
  }
  public static void main( String args[] ) {
      dumpKeys();
  }
}
