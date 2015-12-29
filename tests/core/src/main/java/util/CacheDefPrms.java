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

/**
 * Hydra parameters that let a test writer declaratively specify
 * configurations for and instance of GemFire cache.  
 *
 * @author Lynn Gallinat
 * @since 3.0
 */
public class CacheDefPrms extends BasePrms {

/** (String) Used for specifying cache attributes. 
 */
public static Long cacheSpecs;

/** (String) Used for choosing a particular cache configuration.
 *  It is one of the names defined in cacheSpecs.  
 */
public static Long cacheSpecName;

// ================================================================================
static {
   BasePrms.setValues(CacheDefPrms.class);
}

}
