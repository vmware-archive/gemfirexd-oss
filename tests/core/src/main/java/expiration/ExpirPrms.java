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
package expiration;

import hydra.BasePrms;

public class ExpirPrms extends BasePrms {

/** (boolean) True if the test should use transactions, false otherwise.
 */
public static Long useTransactions;

/** (boolean) True if the test should use client server configuration, false otherwise.
 */
public static Long isBridgeClientConfiguration;


/** (boolean) Number of client vms (in case of client server configuration).
 */
public static Long numClientVms;



// ================================================================================
static {
   BasePrms.setValues(ExpirPrms.class);
}

}
