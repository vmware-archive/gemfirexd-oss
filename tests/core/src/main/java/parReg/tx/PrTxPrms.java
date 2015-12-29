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
package parReg.tx;

import java.util.*;

import hydra.*;
import util.*;

import com.gemstone.gemfire.cache.*;

public class PrTxPrms extends BasePrms {

public static final int NONE = 0;
public static final int PARTITION_RESOLVER = 1;
public static final int CALLBACK_RESOLVER = 2;
public static final int KEY_RESOLVER = 3;

/** 
 *  (String)
 *  
 *  
 *  Defaults to null.
 */ 
public static Long regionConfigNames;
protected static List getRegionConfigNames() {
  Long key = regionConfigNames;
  List val = TestConfig.tasktab().vecAt( key, tab().vecAt( key ) );
  return (val);
}

/** 
 *  (String) List of update regions (see parRegTxQuery.conf)
 */ 
public static Long updateRegions;
protected static List getUpdateRegions() {
  Long key = updateRegions;
  List val = tab().vecAt( key );
  return (val);
}

/** 
 *  (String) Name of 'insert' region (see parRegTxQuery.conf)
 */ 
public static Long insertRegion;
protected static String getInsertRegion() {
  Long key = insertRegion;
  String val = tab().stringAt( key );
  return (val);
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

/** (int) maxKeys (used by PrTxQueryTest.populateRegions)
 *  Defaults to 1000.
 */
public static Long maxKeys;
public static int getMaxKeys() {
  Long key = maxKeys;
  int val = tab().intAt(key, 1000);
  return val;
}

/** (int) Workload based methods can use this param to control the 
 *  number of operations executed.
 *  See PrTxQueryTask.query() and PrTxQueryTask.executeTx()
 */
public static Long numOpsPerTask;

/** (boolean) Whether or not the keySet (for targeting operations) is 
 * local to the tx initiator or remote.  (Used in colocated pr tx tests)
 * Defaults to true (since remote tx proxies are not yet implemented).
 */
public static Long useLocalKeySet;
public static boolean useLocalKeySet() {
   Long key = useLocalKeySet;
   boolean val = tab().booleanAt(key, true);
   return val;
}

/** (boolean) Which VM to kill during parRegIntegrity tests.
 *  When useLocalKeySet = false (target entries in remote dataStores)
 *  we can still kill the localVM (and test TXStateStub callbacks:
 *  afterSendCommit, afterSendRollback) by setting this to true.
 *
 *  When used with useLocalKeySet = true it has no impact.
 *  When used with useLocalKeySet = false, we can kill the local tx vm
 *  even though we are targeting entries hosted by a remote dataStore.
 */
public static Long killLocalTxVm;
public static boolean killLocalTxVm() {
   Long key = killLocalTxVm;
   boolean val = tab().booleanAt(key, false);
   return val;
}

/** (boolean) Whether or not to always target PRs in the tx ops.
 * This is temporary (until mixed remote tx ops are supported).
 * todo@lhughes -- remove this once product support allows
 * Defaults to false (so we get local: mixedRegions)
 */
public static Long alwaysUsePartitionedRegions;
public static boolean alwaysUsePartitionedRegions() {
   Long key = alwaysUsePartitionedRegions;
   boolean val = tab().booleanAt(key, false);
   return val;
}

/** (boolean) Whether or not to execute some tx ops via function execution.
 *  Defaults to false.
 */
public static Long useFunctionExecution;
public static boolean useFunctionExecution() {
   Long key = useFunctionExecution;
   boolean val = tab().booleanAt(key, false);
   return val;
}

/** (boolean) Whether or not to target the same key across all regions.
 *  (This requires that the regions be colocated).
 *  Defaults to false.
 */
public static Long sameKeyColocatedRegions;
public static boolean sameKeyColocatedRegions() {
   Long key = sameKeyColocatedRegions;
   boolean val = tab().booleanAt(key, false);
   return val;
}

/** (String) Designates how CustomPartitioning will be configured
 *           Can be either of: 
 *               PartitionResolver - Defines PartitionPrms-partitionResolver
 *               CallbackResolver - callback implements PartitionResolver interface
 *               KeyResolver - Class used for keys implements PartitionResolver interface
 */
public static Long customPartitionMethod;  
public static int getCustomPartitionMethod() {
  Long key = customPartitionMethod;
  String str = tab().stringAt(key, null);
  
  int method;
  if (str == null) {
    return NONE;
  } else if (str.equalsIgnoreCase("partitionResolver")) {
    return PARTITION_RESOLVER;
  } else if (str.equalsIgnoreCase("callbackResolver")) {
    return CALLBACK_RESOLVER;
  } else if (str.equalsIgnoreCase("keyResolver")) {
    return KEY_RESOLVER;
  } else {
    String s = BasePrms.nameForKey(key) + " has illegal value " + str;
    throw new HydraConfigException(s);
  }
}

public static String customPartitionMethodToString(int method) {
  String s;
  switch (method) {
    case NONE:
      s = "none";
      break;
    case PARTITION_RESOLVER:
      s = "partitionResolver";
      break;
    case CALLBACK_RESOLVER:
      s = "callbackResolver";
      break;
    case KEY_RESOLVER:
      s = "keyResolver";
      break;
    default: throw new HydraInternalException("Unhandled customPartitionMethod " + method);
  }
    return (s);
}

//---------------------------------------------------------------------------
// TransacationListener
//---------------------------------------------------------------------------                                                                                
/**
 *  Class name of transaction listener to use.  Defaults to null.
 */
public static Long txListener;
public static TransactionListener getTxListener() {
  Long key = txListener;
  String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
  if (val != null) {
    try {
      return (TransactionListener)instantiate( key, val );
    } catch( ClassCastException e ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val + " does not implement TransactionListener", e );
    }
  }
  return null;
}

private static Object instantiate( Long key, String classname ) {
  if ( classname == null ) {
    return null;
  }
  try {
    Class cls = Class.forName( classname );
    return cls.newInstance();
  } catch( Exception e ) {
    throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": cannot instantiate " + classname, e );
  }
}



// ================================================================================
static {
   BasePrms.setValues(PrTxPrms.class);
}

}
