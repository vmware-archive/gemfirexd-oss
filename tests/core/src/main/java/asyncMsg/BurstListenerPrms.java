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

package asyncMsg;

import hydra.*;

/**
 *
 *  A class used to set Prms for asyncMsg.BurstListener
 *
 */

public class BurstListenerPrms extends BasePrms {

/**
 *  (Vector) A Vector of sleep times in millis.
 */
public static Long sleepTimeMillis;

/**
 *  (int) The elapsed time (in seconds) that each element of sleepTimes is in effect.
 */
public static Long durationSec;

static {
   setValues(BurstListenerPrms.class);
}

public static void main( String args[] ) {
   dumpKeys();
}

}
