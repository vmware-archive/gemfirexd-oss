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
package cacheLoader.smoke;

import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.LogWriter;

public class Util { 

    //--------------------------------------------------------------------------
    // Constants

    /** Name of the root region of the "master" and "cached"
     * regions. */
    public static final String REGION_NAME = "CacheLoaderTest";

    /** Name of region entry holding value of master data */
    public static final String MASTER_REGION_NAME = "MasterData";

    /** Name of region entry holding data value read by clients */
    public static final String CACHED_REGION_NAME = "CachedData";

    /** The name of the data entry in the master or cache region */
    protected static final String DATA_NAME = "Data";


    private static final String lb = "[";
    private static final String rb = "]";


    //--------------------------------------------------------------------------
    // internal/test utility methods

    protected static String log(Region region, Object key) {
	return (region.getFullPath() + lb + key + rb);
    }

    protected static String log(Region region, Object key, Object value) {
	return (log(region, key) + " = " + value);
    }



} 
