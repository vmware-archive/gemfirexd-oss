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
package diskReg;

import hydra.BasePrms;

public class DiskRegPrms extends BasePrms {

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

/** (String) Used for choosing a valid combinations of disk region attributes that is
 *  specific for this test. Defaults to <code>null</code>.
 **/
public static Long diskAttrSpecs;  
public static String getDiskAttrSpecs() {
  Long key = diskAttrSpecs;
  String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
  return val;
}

/** (String) The operations to do in this concurrent test.
 **/
public static Long entryOperations;  

/** (int) The lower size of the region that will trigger the test
*         choose its operations from the lowerThresholdOperations.
*/

public static Long lowerThreshold;

/** (Vector of Strings) A list of the operations on a region entry that this 
*                       test is allowed to do when the region size falls below lowerThreshold.
*/

public static Long lowerThresholdOperations;


/** (int) The upper size of the  region that will trigger the test
*         chooose its operations from the upperThresholdOperations.
*/

public static Long upperThreshold;

/** (Vector of Strings) A list of operations on a region that this 
*                       test is allowed to do when the region size exceeds the upperThreshold.
*/

public static Long upperThresholdOperations;

/** (boolean) If true, then the object used as the value in the region is 
 *  an instance of ValueHolder that contains other objects. If false, then 
 *  the object used as the value in the region is not a ValueHolder, but is
 *  an object determined by the settings in util.RandomValuesPrms.
 **/
public static Long useComplexObject;  

/** (Vector of Strings)
 */
public static Long diskDirNames;

/** (boolean)
 */
public static Long useBackupDiskDirs;

/** (int) The number of bytes of this number of entries that the region's size
 *  is allowed to deviate from the memLRU capacity.
 */
public static Long numEntriesDeviation;  

/** (int) If we want a workload based test, then set this parameter. If not set
 *  then run until we hit the time limit.
 */
public static Long endTestOnNumKeysInRegion;  

// ================================================================================
static {
   BasePrms.setValues(DiskRegPrms.class);
}

}
