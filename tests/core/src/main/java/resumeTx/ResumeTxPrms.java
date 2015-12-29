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
package resumeTx;

import hydra.BasePrms;

import java.util.Vector;

public class ResumeTxPrms extends BasePrms {
    
    /** (boolean) highAvailability
     *  For resumeTx tests, true indicates that the servers can be recycled (and extra 
     *  allowances must be made for Exception handling (e.g. TransactionDataNodeHasDeparted, etc).
     *  Defaults to false.
     */
    public static Long highAvailability;
    public static boolean getHighAvailability() {
      Long key = highAvailability;
      return tasktab().booleanAt( key, tab().booleanAt( key, false ) );
    }

    /** (boolean) useColocatedEntries
     *  For resumeable transaction tests, indicates that getRandomRegion only return entries
     *  which are primary for this VM (not just the local data set which can include secondaries).
     *  In addition, these entries must return the same hash code (from the PartitionResolver).
     */
    public static Long useColocatedEntries;
    public static boolean getUseColocatedEntries() {
      Long key = useColocatedEntries;
      return tasktab().booleanAt( key, tab().booleanAt( key, false ) );
    }

    /** (int) numColocatedRegions (for colocatedWith testing)
     *  Defaults to 1.
     */
    public static Long numColocatedRegions;
    public static int getNumColocatedRegions() {
      Long key = numColocatedRegions;
      int val = tasktab().intAt( key, tab().intAt(key, 1) );
      return val;
    }
 
    /** (int) The minimum number of times to suspend, doTxOps and resume before committing
    *         the transaction.
    */
    public static Long minExecutions;
    public static int getMinExecutions() {
      Long key = minExecutions;
      int val = tasktab().intAt( key, tab().intAt(key, 1) );
      return val;
    }
 
   /** (int) The number of keys to divide into intervals for known-keys style tests.
    */
   public static Long numIntervalKeys;
   public static int getNumIntervalKeys() {
     Long key = numIntervalKeys;
     int val = tasktab().intAt( key, tab().intAt(key, 1) );
     return val;
   }

   /** (int) The number of new keys to add during known-keys style tests.
    */
   public static Long numNewKeys;
   public static int getNumNewKeys() {
     Long key = numNewKeys;
     int val = tasktab().intAt( key, tab().intAt(key, 1) );
     return val;
   }

   /** (Vector) The region config names to create.
    */
    public static Long regionConfigNames;
    public static Vector getRegionConfigNames() {
      Long key = regionConfigNames;
      Vector val = tasktab().vecAt( key, tab().vecAt(key, null) );
      return val;
   }

// ================================================================================
static {
   BasePrms.setValues(ResumeTxPrms.class);
}

}
