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

package splitBrain;

import hydra.*;
import util.TestHelper;

import java.util.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.admin.*;

/**
 *
 * A class used to store keys for SplitBrain Tests
 *
 */

public class MLRioPrms extends BasePrms {

/**
 *  (int) Number of (non-dynamic) parent root regions to create
 */
public static Long numRootRegions;

/**
 *  (int) Number of dynamicRegions to create in clients
 */
public static Long numDynamicRegions;

/**
 *  (int) Number of rounds of execution (cycling of primary gateway)
 *  required for the test to stop.
 */
public static Long maxExecutions;

/**
 *  (BridgeMembershipListener)
 *  Fully qualified pathname of a BridgeMembershipListener. 
 *  Defaults to null.
 */  
public static Long bridgeMembershipListener;
public static BridgeMembershipListener getBridgeMembershipListener() {
Long key = bridgeMembershipListener;
  String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
  try {
    return (BridgeMembershipListener)instantiate( key, val );
  } catch( ClassCastException e ) {
    throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val + " does not implement BridgeMembershipListener", e );
  }
}

//------------------------------------------------------------------------
// Utility methods
//------------------------------------------------------------------------

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
   BasePrms.setValues(MLRioPrms.class);
}

}
