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

import parReg.ParRegBB;
import parReg.execute.PartitionObjectHolder;

import hydra.*;
import hydra.blackboard.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.*;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import java.util.*;
import util.*;

/**
 * 
 * Test to 
 *
 * @see 
 * @see 
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.1
 */
public class PrTxCustomPartitionTest extends CustomPartitionTest {

  // after Region creation, save the listener information (for use by getNewKey, getExistingKey)
  private ObjectNameListener myListener;
  private int numVms;

  /**
   *  Create the cache and Region.  Check for forced disconnects (for gii tests)
   */
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new PrTxCustomPartitionTest();
      ((PrTxCustomPartitionTest)testInstance).initializeOperationsClient();

      try {
        testInstance.initializePrms();
        testInstance.initialize();
        testInstance.registerFunctions();
      } catch (Exception e) {
        Log.getLogWriter().info("initialize caught Exception " + e + ":" + e.getMessage());
        throw new TestException("initialize caught Exception " + TestHelper.getStackTrace(e));
      }  
    }
  }

  /*
   * Configure cache, regions and prms.
   * Set myListener (for test) so that getExistingKey and getNewKey have the listener instance in hand
   */
  protected void initialize() {
     super.initialize();
     Set regions = CacheHelper.getCache().rootRegions();
     Iterator it = regions.iterator();
     if (it.hasNext()) {
        Region aRegion = (Region)it.next();
        myListener = (ObjectNameListener)aRegion.getAttributes().getCacheListener();
     }
     numVms = TestConfig.getInstance().getTotalVMs()-1;
  }

  /*  Add at least numVm entries to the region, so we've satisfied all hashCodes.
   *  We cannot use addEntry here, since the hashCodes already need to be assigned 
   *  in order for addEntry to retrieve an approriate key (based on hashCode).
   */
  protected void populateRegion(Region aRegion) {
    int numVms = TestConfig.getInstance().getTotalVMs() - 1;
    for (int i = 0; i < numVms; i++) {
      Object key = NameFactory.getNextPositiveObjectName();
      BaseValueHolder anObj = getValueForKey(key);

      Log.getLogWriter().info("addEntry: calling create for key " + key + ", object " + TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath());
      aRegion.create(key, anObj);
      Log.getLogWriter().info("addEntry: done creating key " + key);
    }
  }

//==========================================
// override methods to work on local keySet 
//==========================================

   /** Return a random key currently in the given region.
    *  @param aRegion The region to use for getting an existing key 
    *  @returns A key from the local keySet (built by the ObjectNameListener)
    */
   protected Object getExistingKey(Region aRegion) {
      Object key = null;
      Object[] keySet = myListener.getKeySetArray();
      if (keySet.length > 0) {
         int index = TestConfig.tab().getRandGen().nextInt(0, keySet.length-1);
         key = keySet[index];
         Log.getLogWriter().info("getExistingKey() in " + RemoteTestModule.getMyClientName() + " returning " + key);
         return key;
      } else {
         return null;
      }
   }

   /** Return a new key, never before used in the test.
    *  This must match the existing keySet (in terms of hashing/colocation)
    */
   protected Object getNewKey() {
      List hashCodes = myListener.getHashCodes();
      // get the next key which maps to our VMs hashCodes
      Object key = null;
      int counter = 0;
      int hash = 0;
      do {
        key = NameFactory.getNextPositiveObjectName();
        counter = (int)NameFactory.getCounterForName(key);
        hash = counter % numVms;
      } while (!hashCodes.contains(hash));
      Log.getLogWriter().info("getNewKey() returning key " + key + " with hashCode " + hash + ", local hashCodes = " + hashCodes);
      return key;
   }
}
