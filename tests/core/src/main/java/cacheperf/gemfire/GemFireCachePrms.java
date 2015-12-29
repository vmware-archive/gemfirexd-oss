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

//import com.gemstone.gemfire.cache.*;
import hydra.*;

/**
 *
 *  A class used to store keys for test configuration settings.
 *
 */

public class GemFireCachePrms extends BasePrms {

  //----------------------------------------------------------------------------
  // Cache listeners
  //----------------------------------------------------------------------------

  /**
   *  Whether a cache listener should process events by fetching the key and
   *  value of the object of the event.  Defaults to false.
   */
  public static Long processListenerEvents;
  public static boolean processListenerEvents() {
    Long key = processListenerEvents;
    return tab().booleanAt( key, false );
  }

  /**
   *  Whether a cache listener should process all events or only
   *  events originating from a remote VM.
   *  Defaults to false (process all events).
   */
  public static Long processRemoteEventsOnly;
  public static boolean processRemoteEventsOnly() {
    Long key = processRemoteEventsOnly;
    return tab().booleanAt( key, false);
  }

  /**
   *  Whether a cache listener should log information about processed events.
   *  Defaults to false.
   */
  public static Long logListenerEvents;
  public static boolean logListenerEvents() {
    Long key = logListenerEvents;
    return tab().booleanAt( key, false );
  }

  //----------------------------------------------------------------------------
  // Cache loaders
  //----------------------------------------------------------------------------

  /**
   *  Time to sleep, in milliseconds, during a load, e.g., to simulate going
   *  to an RDB.  Defaults to 0.
   */
  public static Long loaderSleepMs;
  public static int getLoaderSleepMs() {
    Long key = loaderSleepMs;
    int val = tasktab().intAt( key, tab().intAt( key, 0 ) );
    if ( val < 0 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
    return val;
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  static {
      setValues( GemFireCachePrms.class );
  }
  public static void main( String args[] ) {
      dumpKeys();
  }
}
