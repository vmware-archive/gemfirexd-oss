
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

package hct;

import hydra.*;
import util.*;
import com.gemstone.gemfire.cache.CacheListener;

/**
 * Hydra parameters for the BridgeNotify tests.
 * It contains parameters for configuring both the cache server and
 * the the edge client VMs.
 *
 * @author Lynn Hughes-Godfrey
 * @since 5.1
 */
public class BridgeNotifyPrms extends hydra.BasePrms {

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

   /** (int) numKeys (to populate region & to operate on).
    */
   public static Long numKeys;

   /** (String) how clients are to register interest
    *  Can be one of: noInterest, singleKey, oddKeys, evenKeys or allKeys
    */
   public static Long clientInterest;

   /** (String) ClassName for clientListener when registering interest in BridgeNotify methods
    */
   public static Long clientListener;
   
   /** (int) numBridges (to varify event listeners invocation on server)
    */
   public static Long numBridges;
   
   /** (boolean) if bridge dataPolicy is partition or not
    */
   public static Long isPartition;
   
   /** (boolean) if this is a HA test
    */
   public static Long isHATest;
   
   /**
    * Returns the <code>CacheListener</code> that is installed in clients on
    * regions created by this test.
    *
    * @see EventListener
    * @see KeyListListener
    * @see SingleKeyListener
    * @see NoInterestListener
    */
   public static CacheListener getClientListener() {
     String className = TestConfig.tasktab().stringAt( BridgeNotifyPrms.clientListener, TestConfig.tab().stringAt( BridgeNotifyPrms.clientListener, null) );
     CacheListener listener = null;
     if (className != null) {
        listener = (CacheListener)TestHelper.createInstance( className );
     } 
     return listener;
   }

   static {
       setValues( BridgeNotifyPrms.class );
   }
}
