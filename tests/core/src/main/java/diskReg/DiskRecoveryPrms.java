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
package diskReg;

import hydra.*;

public class DiskRecoveryPrms extends BasePrms {

/** (int) Number of entries to create initially
 */
public static Long maxKeys;  

/** (String) Full class name of objectType to generate for the test.
 ** Must implement ConfigurableObject interface (see objects.ConfigurableObject.java).
 **/
public static Long objectType;  

/** (int) Number of operations that must be completed prior to stop
 **/
public static Long minimumNumOpsBeforeStop;

/** (int) Type of stop to carry out.
 **/
public static Long stopType;
public static int getStopType() {
  Long key = stopType;
  String val = TestConfig.tab().stringAt( key );
  return ClientVmMgr.toStopMode( val );
}

// ================================================================================
static {
   BasePrms.setValues(DiskRecoveryPrms.class);
}

}
