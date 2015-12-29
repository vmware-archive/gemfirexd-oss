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
package recovDelay;

import hydra.BasePrms;

public class RecovDelayPrms extends BasePrms {

/** (int) The number of data store vms in the test. This is not set in 
 *        the hydra config file, but is calculated in the hydra config
 *        file and set so the test can read it. 
 */
public static Long numDataStoreVMs;  

/** (int) The number of keys to load into the region.
 */
public static Long initialNumKeys;  

/** (String) The vm stop strategy.
 *           Can be one of: group, single
 */
public static Long stopStrategy;  

/** (String) The vm start strategy.
 *           Can be one of: group, single
 */
public static Long startStrategy;  

/** (int) The total number of vms to stop during the test.
 */
public static Long totalNumVMsToStop;  

/** (String) Can be one of "zero" or "nonZero". "nonZero" will
 *  randomly choose between 1, 2 or 3.
 */
public static Long redundantCopies;  

// ================================================================================
static {
   BasePrms.setValues(RecovDelayPrms.class);
}

}
