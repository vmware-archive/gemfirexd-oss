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
import util.TestHelper;
import com.gemstone.gemfire.cache.*;
import java.util.*;

public class EventPrms extends BasePrms {

/** (int) The maximum number of regions allowed in the test per VM. Due to the concurrent
 *  nature of the tests, this is not a hard limit as the tests may go over this number
 *  slightly. 
 */
public static Long maxRegions;  

// lynn not used; replace with capacity controller
/** (int) The maximum number of objects allowed in a region. 
 */
public static Long maxObjects;  

/** (int) When a new region is created, initialize it with this many names/objects */
public static Long initRegionNumObjects;  

/** (int) The maximum number of values to validate. Useful for a long running test that
 *  cannot take the time to validate all */
public static Long maxNumberToValidate;  

/** (Vector of Strings) A list of the operations on a region entry that this test is allowed to do.
 *  Can be one or more of:
 *     add - add a new key/value to a region.
 *     invalidate - invalidate an entry in a region.
 *     localInvalidate - local invalidate of an entry in a region.
 *     destroy - destroy an entry in a region.
 *     localDestroy - local destroy of an entry in a region.
 *     update - update an entry in a region with a new value.
 *     read - read the value of an entry.
 */
public static Long entryOperations;  

/**
 * Number of objects for performing each putAll operation
 */
public static Long numPutAllObjects;
/** (int) For ProxyEventTest.java, the sleepTime in millis between operations executed by 
 *  HydraTask_doEntryOperations.  This is necessary to allow the ShadowListener to 
 *  keep up with concurrent operations (since the keySet is only known by looking in 
 *  the ShadowRegion and we rely on this for knowing which entries to target with ops).
 */
public static Long sleepTimeMs;
public static int sleepTimeMs() {
  Long key = sleepTimeMs;
  return tasktab().intAt( key, tab().intAt( key, 0) );
}

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
      

/** (Vector of Strings) A list of the operations on a region that this test is allowed to do.
 *  Can be one or more of:
 *     add - add a new subregion.
 *     invalidate - invalidate a region.
 *     destroy - destroy a region.
 *     close - close a region.
 */
public static Long regionOperations;  

/** (boolean) True if the test should randomly lock/unlock a shared lock, just to see
 *  how locking interacts with everything else. The test does not do anything that 
 *  requires the use of a lock. 
 */
public static Long useRandomLocks;  

/** (boolean) True if the test execute operations within a single transaction
 *  Defaults to false
 */
public static Long useTransactions;  
public static boolean useTransactions() {
  Long key = useTransactions;
  return tasktab().booleanAt( key, tab().booleanAt( key, false ));
}

/** (int) In transaction tests, the percentage of transactions to perform commit
 *  (vs. rollback).  Default = 100%.
 */
public static Long commitPercentage;  
public static int getCommitPercentage() {
  Long key = commitPercentage;
  return tasktab().intAt( key, tab().intAt( key, 100 ));
}

/** (String) Class name of the event/region listener to install on the test
 *  regions.  Returns an instance of RegionListener with name = regionName.
 *
 *  @see event.RegionListener.getName()
 *  @see event.RegionListener.setName()
 *  
 */
public static Long regionListener;
public static CacheListener getRegionListener(String regionName) {
   String className = tab().stringAt( EventPrms.regionListener, null );
   CacheListener regionListener = null;

   if (className != null) {
      regionListener = (CacheListener)TestHelper.createInstance( className );
      ((RegionListener)regionListener).setName(regionName);
   }
   return regionListener;
}

/** (Vector of Strings) List of Roles (Required for each Region)
 */
public static Long roles;
public static MembershipAttributes getMembershipAttributes() {
  Vector names = tab().vecAt( EventPrms.roles, new HydraVector() );
  String[] roles = new String[names.size()];
  for (int i=0; i < names.size(); i++) {
    roles[i] = (String)names.get(i);
  }

  MembershipAttributes attrs = null;
  if (roles.length > 0) {
    attrs = new MembershipAttributes( roles,
                                       LossAction.FULL_ACCESS,
                                       ResumptionAction.NONE);
  } 
  return attrs;
}


// ================================================================================
static {
   BasePrms.setValues(EventPrms.class);
}

}
