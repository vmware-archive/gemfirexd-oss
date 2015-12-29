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

package event;

import hydra.*;

public class ListenerPrms extends BasePrms {

/** (Vector of Strings) A list of the operations on listeners that this test is allowed to do.
 *  Can be one or more of:
 *     add - add a new listener
 *     remove - remove an existing listener 
 *     init - intiailize to a base set of listeners 
 *     set - set listenerList to a single listener
 */
public static Long listenerOperations;  

/** (int) maxListeners to be assigned when invoking initCacheListeners.
 */
public static Long maxListeners;

// ================================================================================
static {
   BasePrms.setValues(ListenerPrms.class);
}

}
