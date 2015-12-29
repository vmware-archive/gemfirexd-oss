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

public class CachePrms extends BasePrms {

/** (String) Attribute to use for scope. Can be one of
 *  SCOPE_DISTRIBUTED_ACK, SCOPE_DISTRIBUTED_NO_ACK, SCOPE_GLOBAL, SCOPE_LOCAL */
public static Long scopeAttribute;

/** (String) Attribute to use for mirroring. Can be one of
 *  MIRROR_KEYS, MIRROR_OBJECTS, MIRROR_NONE */
public static Long mirrorAttribute;

/** (String) Attribute to use for dataPolicy. Can be one of
 *   EMPTY
 *   NORMAL
 *   REPLICATE
 *   PERSISTANT_REPLICATE
 */
public static Long dataPolicyAttribute;

/** (String) Attribute to use for interstPolicy. Can be one of
 *   ALL
 *   CACHE_CONTENT
 */
public static Long interestPolicyAttribute;

/** (long) The limit in seconds for how long the test will wait for containsKey or
 *  containsValueForKey to become true when the test expects them to become true */
public static Long keyValueWaitTime;

/** (int)    Region time to live in seconds. 
 *  (String) Region time to live expiration action.
 *           Can be one of destroy, invalidate, localDestroy, localInvalidate 
 *  (int)    Region idle timeout in seconds. 
 *  (String) Region idle timeout expiration action.
 *           Can be one of destroy, invalidate, localDestroy, localInvalidate 
 */
public static Long regionTTLSec;
public static Long regionTTLAction;
public static Long regionIdleTimeoutSec;
public static Long regionIdleTimeoutAction;

/** (int) Entry time to live in seconds. 
 *  (String) Region time to live expiration action.
 *           Can be one of destroy, invalidate, localDestroy, localInvalidate 
 *  (int) Entry idle timeout in seconds. 
 *  (String) Entry idle timeout expiration action.
 *           Can be one of destroy, invalidate, localDestroy, localInvalidate 
 */
public static Long entryTTLSec;
public static Long entryTTLAction;
public static Long entryIdleTimeoutSec;
public static Long entryIdleTimeoutAction;

/** Used for net search timeout */
public static Long searchTimeout;

/** (int) True if the region should be created using a declarative xml file,
 *        false if the region should be created programmatically.
 */
public static Long useDeclarativeXmlFile;  

// ================================================================================
static {
   BasePrms.setValues(CachePrms.class);
}

}
