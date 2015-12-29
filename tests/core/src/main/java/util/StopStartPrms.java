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

import hydra.BasePrms;

public class StopStartPrms extends BasePrms {

/** (String) The possible stop modes for stopping a VM. Can be any of
 *           MEAN_EXIT, MEAN_KILL, NICE_EXIT, NICE_KILL.
 */
public static Long stopModes;  

/** (int) The number of VMs to stop (then restart) at a time.
 */
public static Long numVMsToStop;  

// ================================================================================
static {
   BasePrms.setValues(StopStartPrms.class);
}

}
