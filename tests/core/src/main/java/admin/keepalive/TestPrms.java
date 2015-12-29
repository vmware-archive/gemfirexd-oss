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

package admin.keepalive;

import hydra.*;
//import util.*;

/**
*
* A class used to store keys for Admin API region "keep alive" Tests
*
*/

public class TestPrms extends BasePrms {

    //---------------------------------------------------------------------
    // Default Values

    //---------------------------------------------------------------------
    // Test-specific parameters

    /** (boolean) controls whether CacheLoader is defined
     */
    public static Long defineCacheLoaderRemote; 

    /* 
     * Returns boolean value of TestPrms.defineCacheLoaderRemote.
     * Defaults to false.
     */
    public static boolean getDefineCacheLoaderRemote() {
	Long key = defineCacheLoaderRemote;
	return (tasktab().booleanAt(key, tab().booleanAt(key, false)));
    }

}
