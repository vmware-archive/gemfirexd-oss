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
package mirror;

import hydra.BasePrms;

public class MirrorPrms extends BasePrms {

/** (int) The class name of the listener to use.
 */
public static Long listenerClassName;  

/** (int) The class name of the listener to use.
 */
public static Long expectErrorOnRegionConfig;  

/** (int) The number of keys/values to mirror to terminate the test
 */
public static Long totalNumObjectsToMirror;  

/** (boolean) True if the test execute operations within a single transaction
 *  Defaults to false
 */
public static Long useTransactions;  
public static boolean useTransactions() {
  Long key = useTransactions;
  return tasktab().booleanAt( key, tab().booleanAt( key, false ));
}

// ================================================================================
static {
   BasePrms.setValues(MirrorPrms.class);
}

}
