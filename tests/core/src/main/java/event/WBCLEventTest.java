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

import hydra.blackboard.*;
import hydra.*;
import event.*;
import util.*;

import com.gemstone.gemfire.cache.*;

import java.util.*;

/**
 * Extends event.EventTest to test GatewayEventListeners.  This requires the use of 
 * hydra Cache and RegionHelper classes (vs. RegionDefinition), so this class provides
 * additional INITTASKS for Gateway and Region configuration.
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.6.2
 */
public class WBCLEventTest extends EventTest {

  public Region rootRegion;

  /**
   * Starts a gateway hub in a VM that previously created one, after creating
   * gateways.
   */
  public static void startGatewayHubTask() {
    ((WBCLEventTest)eventTest).startGatewayHub(ConfigPrms.getGatewayConfig());
  }

  /**
   * Creates a gateway hub using the {@link CacheServerPrms}.
   */
  protected void createGatewayHub() {
    String gatewayHubConfig = ConfigPrms.getGatewayHubConfig();
    if (gatewayHubConfig != null) {
      GatewayHubHelper.createGatewayHub(gatewayHubConfig);
    }
  }

  /**
   * Starts a gateway hub in a VM that previously created one, after creating
   * gateways.
   */
  protected void startGatewayHub(String gatewayConfig) {
    GatewayHubHelper.addWBCLGateway(gatewayConfig);
    GatewayHubHelper.startGatewayHub();
  }

  //============================================================================
  // INITTASKS (overrides)
  //============================================================================

 /** 
  *  Cache initialization for peer gateways
  */
  public static synchronized void HydraTask_initialize() {
    if (eventTest == null) {
      eventTest = new WBCLEventTest();
      ((WBCLEventTest)eventTest).initialize();
    } 
  }

  /**
   * @see #HydraTask_initialize (set up for EventTest methods)
   */
  protected void initialize() {
      CacheHelper.createCache(ConfigPrms.getCacheConfig());
      rootRegion = RegionHelper.createRegion(ConfigPrms.getRegionConfig());
      ((WBCLEventTest)eventTest).createGatewayHub();

      useTransactions = EventPrms.useTransactions();
      isSerialExecution = EventBB.isSerialExecution();
      isCarefulValidation = isCarefulValidation || isSerialExecution;
      numVMs = 0;
      Vector gemFireNamesVec = TestConfig.tab().vecAt(GemFirePrms.names);
      Vector numVMsVec = TestConfig.tab().vecAt(ClientPrms.vmQuantities);
      if (gemFireNamesVec.size() == numVMsVec.size()) {
          for (int i = 0; i < numVMsVec.size(); i++) {
              numVMs = numVMs + (new Integer(((String)numVMsVec.elementAt(i)))).intValue();
          }
      } else {
          numVMs = new Integer((String)(numVMsVec.elementAt(0))).intValue() * gemFireNamesVec.size();
      }
      Log.getLogWriter().info("numVMs is " + numVMs);
      minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
      minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
      maxRegions = TestConfig.tab().intAt(EventPrms.maxRegions, -1);
      maxObjects = TestConfig.tab().intAt(EventPrms.maxObjects, -1);
      randomValues = new RandomValues();
      isListenerTest = false;
      useCounters = isSerialExecution;
  }

  //============================================================================
  // TASKS (overrides)
  //============================================================================

  /**
   * Performs randomly selected operations (add, invalidate, etc.) on
   * the root region based on the weightings in {@link
   * EventPrms#entryOperations}.  The operations will continue to be
   * performed until the {@linkplain
   * TestHelperPrms#minTaskGranularitySec minimum task granularity}
   * has been reached.
   */
  public static void HydraTask_doEntryOperations() {
      ((WBCLEventTest)eventTest).doEntryOperations();
  }

  public void doEntryOperations() {
      ((WBCLEventTest)eventTest).doEntryOperations(rootRegion);
  }

// todo@lhughes - if we want to include get/load, then we'll need to enable override
// addObject().  In addition, modify read() so we don't accidently generate a get/load/create event
// for a destroyed entry
//protected void addObject(Region aRegion, boolean logAddition) {
//  String name = NameFactory.getNextPositiveObjectName();
//  Object anObj = getObjectToAdd(name);
//  String callback = createCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();
//
//  if (TestConfig.tab().getRandGen().nextBoolean()) { // get with a loader
//    if (logAddition) {
//      Log.getLogWriter().info("addObject: calling get/load for name " + name + ", object " +
//                              " callback is " + callback + ", region is " + aRegion.getFullPath());
//      anObj = aRegion.get(name, callback);   
//    }
//  } else {
//    if (logAddition) {
//      Log.getLogWriter().info("addObject: calling put for name " + name + ", object " +
//        TestHelper.toString(anObj) + " callback is " + callback + ", region is " + aRegion.getFullPath());
//    }
//    try {
//        aRegion.put(name, anObj, callback);
//    } catch (RegionDestroyedException e) {
//        handleRegionDestroyedException(aRegion, e);
//    } catch (RegionRoleException e) {
//        throw e;
//    } catch (PartitionedRegionStorageException e) {
//        if (isSerialExecution) {
//           throw new TestException(TestHelper.getStackTrace(e));
//        } else {
//           Log.getLogWriter().info("Caught expected exception " + e.getMessage() + ";continuing test"); 
//        }
//    } catch (Exception e) {
//        throw new TestException(TestHelper.getStackTrace(e));
//    }
//  }
//  long numPut = EventBB.incrementCounter("EventBB.NUM_CREATE", EventBB.NUM_CREATE);
//}

  /**
   * Returns the "updated" value of the object with the given
   * <code>name</code>.
   *
   * @see BaseValueHolder#getAlternateValueHolder
   */
  protected Object getUpdateObject(String name) {
      BaseValueHolder anObj = null;
      BaseValueHolder newObj = null;
      try {
        anObj = (BaseValueHolder)rootRegion.get(name);
      } catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      newObj = (anObj == null) ? new ValueHolder(name, randomValues) :
        anObj.getAlternateValueHolder(randomValues);
      return newObj;
  }

  /**
   * Check that no Listener Exceptions were posted to the BB 
   */
  public static void HydraTask_checkForEventErrors() {
    TestHelper.checkForEventError(EventBB.getBB());
  }
}
