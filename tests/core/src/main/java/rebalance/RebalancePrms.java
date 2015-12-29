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
package rebalance;

import java.util.*;
import hydra.*;

import com.gemstone.gemfire.cache.control.*;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.ResourceListener;

public class RebalancePrms extends BasePrms {

/** (String[]) An array of Strings of the extraArgs to be passed in to the 
 *  CacheServer VM when invoked via 
 *  hydra.CacheServerHelper.startCacheServer()
 */
public static Long extraCacheServerArgs;  
public static String[] getExtraCacheServerArgs() {
  Long key = extraCacheServerArgs;

  Vector argList = TestConfig.tasktab().vecAt( key, TestConfig.tab().vecAt( key, new HydraVector()) );
  String[] val = new String[ argList.size() ];
  for (int i = 0; i < argList.size(); i++) {
    val[i] = (String)argList.elementAt(i);
  }
  return val;
}

/**
 *  (boolean) Whether or not to verify RebalanceResults (isBalanceImproved).
 *  Note that this can only be done when the rebalance task is done 
 *  serially (or only assigned to one thread) and membership is not changing
 *  during rebalance.  Defaults to false.
 */
public static Long verifyBalance;
public static boolean verifyBalance() {
   Long key = verifyBalance;
   boolean val = TestConfig.tasktab().booleanAt(key, TestConfig.tab().booleanAt(key, false));
   return val;
}

/**
 *  (boolean) Whether or not to to wait for networkDropComplete
 *  prior to returning from RebalanceTest.HydraTask_rebalance.
 *  Used by serialPRNetDown tests.  Defaults to false.
 */
public static Long waitForNetworkDrop;
public static boolean waitForNetworkDrop() {
   Long key = waitForNetworkDrop;
   boolean val = TestConfig.tasktab().booleanAt(key, TestConfig.tab().booleanAt(key, false));
   return val;
}

/**
  * (String) Class name of the InternalResourceManager.ResourceObserver
  * to install on the cache.  Used by rebalance tests.  Defaults to null.
  * This provides a test hook callback when rebalance or recovery is
  * started/finished.
  */
public static Long resourceObserver;
public static InternalResourceManager.ResourceObserver getResourceObserver() {
  Long key = resourceObserver;
  String str = TestConfig.tasktab().stringAt(key, TestConfig.tab().stringAt( key, null ));

  InternalResourceManager.ResourceObserver observer = null;
 
  try {
    observer = (InternalResourceManager.ResourceObserver)instantiate(key, str);
  } catch( ClassCastException e ) {
    throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + str + " does not implement ResourceObserver", e );
  }

  return observer;
}

/** (int) numRegions (for multipleRegions and/or colocatedWith testing)
 *  Defaults to 1.
 */
public static Long numRegions;
public static int getNumRegions() {
  Long key = numRegions;
  int val = tasktab().intAt( key, tab().intAt(key, 1) );
  return val;
}

/** (int) actionDelaySecs (killTargetVm.conf will use this for killing the 
 *  the rebalancer VM after processing the rebalancingStarted event.
 *  We need to delay a bit to let rebalancing get started though.
 *  Default value is 0.
 */
public static Long actionDelaySecs;
public static int getActionDelaySecs() {
  Long key = actionDelaySecs;
  int val = tasktab().intAt( key, tab().intAt(key, 0) );
  return val;
}

/** (String) Which ResourceObserver event triggers a test action.  
 *  For example, if we want to kill the rebalancer during a rebalance, 
 *  the event will be rebalanceStarted.  targetEvent is also used to
 *  time the 'cancel' operation.
 *  Valid values: rebalancingStarted, movingBucket, movingPrimary
 *  Also: skipSimulate (used to avoid extra ResourceObserver messages
 *        associated with simulate).  See rebalance/hct_failover.conf
 *  Defaults to null.
 */
public static Long targetEvent;
public static String getTargetEvent() {
   Long key = targetEvent;
   String val = tasktab().stringAt( key, tab().stringAt(key, null) );
   return val;
}

/** (int) The number of keys to load.
 */
public static Long numKeys;
public static int getNumKeys() {
   Long key = numKeys;
   int val = tasktab().intAt( key, tab().intAt(key, -1) );
   return val;
}

/** (boolean) If true, then use HashKey to put to create uneven buckets.
 */
public static Long useHashKey;
public static boolean getUseHashKey() {
   Long key = useHashKey;
   boolean val = tasktab().booleanAt( key, tab().booleanAt(key, false) );
   return val;
}

/** (int) The limit of the number of hashCodes that can be used for HashKeys.
 */
public static Long hashKeyLimit;
public static int getHashKeyLimit() {
   Long key = hashKeyLimit;
   int val = tasktab().intAt( key, tab().intAt(key, -1) );
   return val;
}

/** (int) The local max memory setting for the PR.
 *  Valid values: 
 */
public static Long localMaxMemory;
public static String getLocalMaxMemory() {
   Long key = localMaxMemory;
   String val = tasktab().stringAt( key, tab().stringAt(key, null) );
   return val;
}

/** (String) Action (cancel or kill) to perform at point targetEvent
 *  during Rebalance.  Valid values: cancel, kill.
 *  Defaults to none.
 */
public static Long rebalanceAction;
public static String getRebalanceAction() {
   Long key = rebalanceAction;
   String val = tasktab().stringAt( key, tab().stringAt(key, "none") );
   return val;
}

/** (boolean) Whether data is required to move during rebalancing.
 *  true => if no data is moved/transferred, throw an Exception
 *  false => log a warning if no data moved/transferred during rebalance.
 */
public static Long actionRequired;

 /*--------------------------- Utility methods ------------------*/

  private static Object instantiate( Long key, String classname ) {
    if ( classname == null ) {
      return null;
    }

    try {
      Class cls = Class.forName( classname );
      return cls.newInstance();
    } catch( Exception e ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key
  ) + ": cannot instantiate " + classname, e );
    }
  }

  
// ================================================================================
static {
   BasePrms.setValues(RebalancePrms.class);
}

}
