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
package capCon;

import hydra.BasePrms;

public class CapConPrms extends BasePrms {

/** (int) The size for byte[] objects, used as values in regions that have 
 *  memory evictors installed.
 */
public static Long maximumEntries;  

/** Parameters for constructing a MemLRUCapacityController.
 *  (int) The maximumMegabytes parameter for creating a new MemLRUCapacityController.
 */
public static Long maximumMegabytes;  

/** (int) The size for byte[] objects, used as values in regions that have 
 *  MemLRUCapacityControllers installed.
 */
public static Long byteArraySize;  

/** (boolean) Indicates if byte[] should be filled with bytes.
 */
public static Long fillByteArray;  

/** (int) Specifies an upper limit on the number of bytes allowable as overage
 *  for memlru. This number is the number of bytes higher than the maximumMegabytes 
 *  setting on the controller (ie. upperThreshold = maximumMegabytes + upperLimitDelta). 
 *  The size of the region must not go over this threshold.
 */
public static Long upperLimitDelta;  

/** (int) Specifies a lower limit on the number of bytes that must be present
 *  in a region (for memLRU) for eviction to occur. This number is the number of
 *  bytes lower than the maximumMegabytes setting on the controller 
 *  (ie. lowerThreshold = maximumMegabytes - lowerLimitDelta). Evictions must occur 
 *  at this threshold or higher.
 */
public static Long lowerLimitDelta;  

/** (int) The number of seconds to run the end task.
 */
public static Long endTaskSecToRun;  

/** (int) Since eviction of objects by capacity controllers is "roughly" 
 *  the least recently used object, this specifies how far an evicted object
 *  can be from the actual least recently used object. For example, a 3 means
 *  that an evicted object can be the 1st, 2nd or 3rd least recently used
 *  objects and the test will allow it.
 */
public static Long LRUAllowance;  

/** (boolean) If true, the test will install a CacheLoader and do gets to
 *  fill the region, false if the test should do puts to fill the region.
 */
public static Long useCacheLoader;  

/** (boolean) If true, the test will read the value of useCacheLoader once
 *  and use that value throughout the test. This is useful for specifying
 *  useCacheLoader with a ONEOF; setting this to true will read the ONEOF
 *  value once and use it throughout the entire test. A setting of false
 *  would get a potentially different value of useCacheLoader each time 
 *  it is read in the test (if ONEOF is used), thus making the test randomly 
 *  alternate between using a cache loader or not within one run.
 */
public static Long fixUseCacheLoader;  

/** (int) The multiplier for the bound point when determining the dynamic
 *  capacity of a region.
 */
public static Long boundsMultiplier;  

/** (boolean) True if dynamic capacity changes during the test should be
 *  random, false if they should gradually climb and fall.
 */
public static Long randomCapacityChanges;  

/** (boolean) True if the test should use transactions, false otherwise.
 */
public static Long useTransactions;  

/** (int) The number of objects to add to a region during a transactions.
 *        Currently used only in evict tests.
 */
public static Long numToAddInTx;  

// ================================================================================
static {
   BasePrms.setValues(CapConPrms.class);
}

}
