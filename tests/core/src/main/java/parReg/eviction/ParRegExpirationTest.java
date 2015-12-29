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
package parReg.eviction;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.Log;
import hydra.PoolHelper;
import hydra.RegionDescription;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;
import util.TxHelper;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.TimeoutException;

import expiration.ExpirPrms;
import expiration.ExpirationBB;

public class ParRegExpirationTest {

  // the one instance of ExpirationTest
  static protected ParRegExpirationTest testInstance;

  protected static Cache theCache;

  // the region names used in this test;
  protected static String[] regionNames;
  
  protected static ArrayList bridgeRegionDescriptions = new ArrayList();

  protected static ArrayList edgeRegionDescriptions = new ArrayList();

  protected static final int TTLDestroyIndex = 0;

  protected static final int TTLInvalIndex = 1;

  protected static final int IdleTODestroyIndex = 2;

  protected static final int IdleTOInvalIndex = 3;

  protected static final int CustomTTLDestroyIndex = 4;

  protected static final int CustomTTLInvalIndex = 5;

  protected static final int CustomIdleTODestroyIndex = 6;

  protected static final int CustomIdleTOInvalIndex = 7;

  protected static final int numRegions = 8;

  static {
    regionNames = new String[numRegions];
    regionNames[TTLDestroyIndex] = "TTLDestroy";
    regionNames[TTLInvalIndex] = "TTLInval";
    regionNames[IdleTODestroyIndex] = "IdleTODestroy";
    regionNames[IdleTOInvalIndex] = "IdleTOInval";
    regionNames[CustomTTLDestroyIndex] = "CustomTTLDestroy";
    regionNames[CustomTTLInvalIndex] = "CustomTTLInval";
    regionNames[CustomIdleTODestroyIndex] = "CustomIdleTODestroy";
    regionNames[CustomIdleTOInvalIndex] = "CustomIdleTOInval";

  }

  /**
   * Initialize all 4 RegionDefintion instances for this run, and write them to
   * the blackboard.
   */
  public static void StartTask_initialize() {
    for (int i = 0; i < regionNames.length; i++) {
      RegionDescription regDescription = RegionHelper
          .getRegionDescription(regionNames[i]);
      ExpirationBB.getBB().getSharedMap().put(regionNames[i], regDescription);
    }
  }
  
  /**
   * Creates and initializes the singleton instance of ParRegExpirationTest. All
   * regions are created.
   */
  public synchronized static void HydraTask_initializeControlThread() {
    if (testInstance == null) {
      testInstance = new ParRegExpirationTest();
      testInstance.initializeControlThread();
    }
  }
  
  
  /**
   * Creates and initializes the singleton instance of ParRegExpirationTest. All
   * regions are created.
   */
  public synchronized static void HydraTask_initServers() {
    if (testInstance == null) {
      testInstance = new ParRegExpirationTest();

      Vector regionDescriptionNames = TestConfig.tab().vecAt(RegionPrms.names,
          null);
      Iterator iterator = regionDescriptionNames.iterator();
      while (iterator.hasNext()) {
        String regionDescription = (String)iterator.next();
        if (regionDescription.startsWith("bridge")) {
          bridgeRegionDescriptions.add(regionDescription);
        }
        else if (regionDescription.startsWith("edge")) {
          edgeRegionDescriptions.add(regionDescription);
        }
        else {
          throw new TestException(
              "Test configuration issue - region name not valid "
                  + regionDescription);
        }
      }

      Log.getLogWriter().info(
          "bridgeRegionDescriptions are " + bridgeRegionDescriptions);
      Log.getLogWriter().info(
          "edgeRegionDescriptions are " + edgeRegionDescriptions);

      testInstance.initializeServers();
      BridgeHelper.startBridgeServer("bridge");
    }
  }
  
  /**
   * Creates and initializes the singleton instance of ParRegExpirationTest. All
   * regions are created.
   */
  public synchronized static void HydraTask_initClients() {
    if (testInstance == null) {
      testInstance = new ParRegExpirationTest();

      Vector regionDescriptionNames = TestConfig.tab().vecAt(RegionPrms.names,
          null);
      Iterator iterator = regionDescriptionNames.iterator();
      while (iterator.hasNext()) {
        String regionDescription = (String)iterator.next();
        if (regionDescription.startsWith("bridge")) {
          bridgeRegionDescriptions.add(regionDescription);
        }
        else if (regionDescription.startsWith("edge")) {
          edgeRegionDescriptions.add(regionDescription);
        }
        else {
          throw new TestException(
              "Test configuration issue - region name not valid "
                  + regionDescription);
        }
      }

      testInstance.initializeClients();
    }
  }

  /**
   * Hydra task to control the thread that has an expiration action set in the
   * region TTLDestroy
   */
  public static void HydraTask_controlEntryTTLDestroy() {
    testInstance.controlEntryTTLDestroy();
  }

  /**
   * Hydra task to control the thread that has an expiration action set in the
   * region TTLInval
   */
  public static void HydraTask_controlEntryTTLInval() {
    testInstance.controlEntryTTLInval();
  }

  /**
   * Hydra task to control the thread that has an expiration action set in the
   * region IdleTODestroy
   */
  public static void HydraTask_controlEntryIdleTODestroy() {
    testInstance.controlEntryIdleTODestroy();
  }

  /**
   * Hydra task to control the thread that has an expiration action set in the
   * region IdleTOInval
   */
  public static void HydraTask_controlEntryIdleTOInval() {
    testInstance.controlEntryIdleTOInval();
  }

  /**
   * Hydra task to control the thread that has expiration action set in the
   * region TTLDestroy (Custom expiry)
   */
  public static void HydraTask_customEntryTTLDestroy() {
    testInstance.customEntryTTLDestroy();
  }

  /**
   * Hydra task to control the thread that has expiration action set in the
   * region TTLInvalidate (Custom expiry)
   */
  public static void HydraTask_customEntryTTLInval() {
    testInstance.customEntryTTLInval();
  }

  /**
   * Hydra task to control the thread that has expiration action set in the
   * region IdleTODestroy (Custom expiry)
   */
  public static void HydraTask_customEntryIdleTODestroy() {
    testInstance.customEntryIdleTODestroy();
  }

  /**
   * Hydra task to control the thread that has expiration action set in the
   * region IdleTOInvalidate (Custom expiry)
   */
  public static void HydraTask_customEntryIdleTOInval() {
    testInstance.customEntryIdleTOInval();
  }

  /**
   * Create the regions specified
   */
  protected void initializeControlThread() {

    if (theCache == null) {
      theCache = CacheHelper.createCache("cache1");
    }
    for (int i = 0; i < regionNames.length; i++) {
      RegionDescription regDescription = (RegionDescription)ExpirationBB
          .getBB().getSharedMap().get(regionNames[i]);
      RegionHelper.createRegion(regionNames[i], regDescription.getName());
    }
  }
  
  
  /**
   * Create the regions specified
   */
  protected void initializeServers() {
    if (theCache == null) {
      theCache = CacheHelper.createCache("cache1");
    }
    Iterator iterator = bridgeRegionDescriptions.iterator();
    while (iterator.hasNext()) {
      String regionDescriptionName = (String)iterator.next();
      RegionDescription regDescription = RegionHelper
          .getRegionDescription(regionDescriptionName);
      String regionName = regDescription.getRegionName();
      RegionHelper.createRegion(regionName, regionDescriptionName);
    }
  }

  /**
   * Create the regions specified
   */
  public void initializeClients() {
    if (theCache == null) {
      theCache = CacheHelper.createCache("cache1");
    }

    Iterator iterator = edgeRegionDescriptions.iterator();
    while (iterator.hasNext()) {
      String regionDescriptionName = (String)iterator.next();
      RegionDescription regDescription = RegionHelper
          .getRegionDescription(regionDescriptionName);

      RegionAttributes attributes = RegionHelper
          .getRegionAttributes(regDescription.getName());

      String poolName = attributes.getPoolName();
      if (poolName != null) {
        PoolHelper.createPool(poolName);
      }

      String regionName = regDescription.getRegionName();
      Region aRegion = theCache.createRegion(regionName, attributes);
      aRegion.registerInterest("ALL_KEYS");
    }
  }

  /**
   * Controls the thread that has an expiration action set for the entry in the
   * region TTLDestroy.
   */
  public void controlEntryTTLDestroy() {
    String regionName = regionNames[TTLDestroyIndex];
    Region aRegion = CacheHelper.getCache().getRegion(regionName);
    String keyPrefix = regionName + "_";
    int numKeys = populateRegion(aRegion, keyPrefix);
    Object key = regionName + "_numKeys";
    Log.getLogWriter().info(
        "Putting " + key + ", " + numKeys + " into ExpirationBB");
    ExpirationBB.getBB().getSharedMap().put(key, new Integer(numKeys));
    
    int expectedVmsForEvents = 1;

    if (TestConfig.tab()
        .booleanAt(ExpirPrms.isBridgeClientConfiguration, false)) {
      expectedVmsForEvents = TestConfig.tab().intAt(ExpirPrms.numClientVms) + 1;
    }

    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterCreateEvents_TTLDestroy",
        ParRegExpirationBB.numAfterCreateEvents_TTLDestroy, numKeys
            * expectedVmsForEvents, true, 600000);

    ParRegExpirationBB.getBB().getSharedCounters().increment(
        ParRegExpirationBB.numPopulationTask_Completed);

    // Wait for remote caches to receive all destroy events
    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterDestroyEvents_TTLDestroy",
        ParRegExpirationBB.numAfterDestroyEvents_TTLDestroy, numKeys
            * expectedVmsForEvents, true, 600000);

    // Check that all were destroyed

    int regionSize = aRegion.keySet().size();
    if (regionSize != 0) {
      throw new TestException(
          "Expected region to have all its entries destroyed, but it has "
              + regionSize + " keys");
    }
    else {
      Log
          .getLogWriter()
          .info(
              "As expected all entries in the region are destroyed in TTL with Destroy action");
    }

    ParRegExpirationBB.getBB().printSharedCounters();
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterCreateEvents_TTLDestroy);
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterDestroyEvents_TTLDestroy);
  }

  /**
   * Controls the thread that has an expiration action set in the entry for
   * region TTLInval
   */
  public void controlEntryTTLInval() {
    String regionName = regionNames[TTLInvalIndex];
    Region aRegion = CacheHelper.getCache().getRegion(regionName);
    String keyPrefix = regionName + "_";
    int numKeys = populateRegion(aRegion, keyPrefix);
    Object key = regionName + "_numKeys";
    Log.getLogWriter().info(
        "Putting " + key + ", " + numKeys + " into ExpirationBB");
    ExpirationBB.getBB().getSharedMap().put(key, new Integer(numKeys));
    
    int expectedVmsForEvents = 1;

    if (TestConfig.tab()
        .booleanAt(ExpirPrms.isBridgeClientConfiguration, false)) {
      expectedVmsForEvents = TestConfig.tab().intAt(ExpirPrms.numClientVms) + 1;
    }

    // Wait for this VM to receive all create events
    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterCreateEvents_TTLInvalidate",
        ParRegExpirationBB.numAfterCreateEvents_TTLInvalidate, numKeys
            * expectedVmsForEvents, true, 600000);

    ParRegExpirationBB.getBB().getSharedCounters().increment(
        ParRegExpirationBB.numPopulationTask_Completed);

    // Wait for this VM to receive all invalidate events
    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterInvalidateEvents_TTLInvalidate",
        ParRegExpirationBB.numAfterInvalidateEvents_TTLInvalidate, numKeys
            * expectedVmsForEvents, true, 600000);

    Iterator it = aRegion.keySet().iterator();
    while (it.hasNext()) { // it's ok to do gets, since no other cache has a
      // value either
      key = it.next();
      try {
        Object value = aRegion.get(key);
        if (value != null)
          throw new TestException("Found key " + key + " with non-null value "
              + value);
      }
      catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }

    Log
        .getLogWriter()
        .info(
            "As expected all entries in the region are invalidated in TTL with Invalidate action, region size is "
                + aRegion.size());

    ParRegExpirationBB.getBB().printSharedCounters();
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterCreateEvents_TTLInvalidate);
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterInvalidateEvents_TTLInvalidate);
  }

  /**
   * Controls the thread that has an expiration action set in the entry for
   * region IdleTODestroy
   */
  public void controlEntryIdleTODestroy() {
    String regionName = regionNames[IdleTODestroyIndex];
    Region aRegion = CacheHelper.getCache().getRegion(regionName);
    String keyPrefix = regionName + "_";
    int numKeys = populateRegion(aRegion, keyPrefix);
    Object key = regionName + "_numKeys";
    Log.getLogWriter().info(
        "Putting " + key + ", " + numKeys + " into ExpirationBB");
    ExpirationBB.getBB().getSharedMap().put(key, new Integer(numKeys));
    
    int expectedVmsForEvents = 1;

    if (TestConfig.tab()
        .booleanAt(ExpirPrms.isBridgeClientConfiguration, false)) {
      expectedVmsForEvents = TestConfig.tab().intAt(ExpirPrms.numClientVms) + 1;
    }

    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterCreateEvents_IdleTODestroy",
        ParRegExpirationBB.numAfterCreateEvents_IdleTODestroy, numKeys
            * expectedVmsForEvents, true, 600000);

    ParRegExpirationBB.getBB().getSharedCounters().increment(
        ParRegExpirationBB.numPopulationTask_Completed);

    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterDestroyEvents_IdleTODestroy",
        ParRegExpirationBB.numAfterDestroyEvents_IdleTODestroy, numKeys
            * expectedVmsForEvents, true, 600000);

    // Check that all were destroyed

    int regionSize = aRegion.keys().size();
    if (regionSize != 0) {
      throw new TestException(
          "Expected region to have all its entries destroyed, but it has "
              + regionSize + " keys");
    }
    else {
      Log
          .getLogWriter()
          .info(
              "As expected all entries in the region are destroyed in Idle TO  with Destroy action");
    }

    ParRegExpirationBB.getBB().printSharedCounters();
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterCreateEvents_IdleTODestroy);
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterDestroyEvents_IdleTODestroy);
  }

  /**
   * Controls the thread that has an expiration action set in the entry for
   * region IdleTOInval
   */
  public void controlEntryIdleTOInval() {
    String regionName = regionNames[IdleTOInvalIndex];
    Region aRegion = CacheHelper.getCache().getRegion(regionName);
    String keyPrefix = regionName + "_";
    int numKeys = populateRegion(aRegion, keyPrefix);
    Object key = regionName + "_numKeys";
    Log.getLogWriter().info(
        "Putting " + key + ", " + numKeys + " into ExpirationBB");
    ExpirationBB.getBB().getSharedMap().put(key, new Integer(numKeys));
    
    int expectedVmsForEvents = 1;

    if (TestConfig.tab()
        .booleanAt(ExpirPrms.isBridgeClientConfiguration, false)) {
      expectedVmsForEvents = TestConfig.tab().intAt(ExpirPrms.numClientVms) + 1;
    }

    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterCreateEvents_IdleTOInvalidate",
        ParRegExpirationBB.numAfterCreateEvents_IdleTOInvalidate, numKeys
            * expectedVmsForEvents, true, 600000);

    ParRegExpirationBB.getBB().getSharedCounters().increment(
        ParRegExpirationBB.numPopulationTask_Completed);

    // Wait for remote caches to receive all invalidate events
    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterInvalidateEvents_IdleTOInvalidate",
        ParRegExpirationBB.numAfterInvalidateEvents_IdleTOInvalidate, numKeys
            * expectedVmsForEvents, true, 600000);

    Iterator it = aRegion.keySet().iterator();
    while (it.hasNext()) { // it's ok to do gets, since no other cache has a
      // value either
      key = it.next();
      try {
        Object value = aRegion.get(key);
        if (value != null)
          throw new TestException("Found key " + key + " with non-null value "
              + value);
      }
      catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }

    Log
        .getLogWriter()
        .info(
            "As expected all entries in the region are invalidated in Idle TO  with Invalidate action, region size is "
                + aRegion.size());

    ParRegExpirationBB.getBB().printSharedCounters();
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterCreateEvents_IdleTOInvalidate);
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterInvalidateEvents_IdleTOInvalidate);
  }

  /**
   * Controls the thread that has an expiration action set for the entry in the
   * region TTLDestroy. (custom expiry)
   */
  public void customEntryTTLDestroy() {
    String regionName = regionNames[CustomTTLDestroyIndex];
    Region aRegion = CacheHelper.getCache().getRegion(regionName);

    String expiryKeyPrefix = "Expire_" + regionName + "_";
    int expireNumKeys = populateRegion(aRegion, expiryKeyPrefix);
    Object expireKey = regionName + "_expireNumKeys";
    Log.getLogWriter().info(
        "Putting " + expireKey + ", " + expireNumKeys + " into ExpirationBB");
    ExpirationBB.getBB().getSharedMap().put(expireKey,
        new Integer(expireNumKeys));

    String noExpiryKeyPrefix = "NotExpire_" + regionName + "_";
    int noExpireNumKeys = populateRegion(aRegion, noExpiryKeyPrefix);
    Object noExpireKey = regionName + "_noExpireNumKeys";
    Log.getLogWriter().info(
        "Putting " + noExpireKey + ", " + noExpireNumKeys
            + " into ExpirationBB");
    ExpirationBB.getBB().getSharedMap().put(noExpireKey,
        new Integer(noExpireNumKeys));
    
    int expectedVmsForEvents = 1;

    if (TestConfig.tab()
        .booleanAt(ExpirPrms.isBridgeClientConfiguration, false)) {
      expectedVmsForEvents = TestConfig.tab().intAt(ExpirPrms.numClientVms) + 1;
    }

    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterCreateEvents_CustomExpiryTTLDestroy",
        ParRegExpirationBB.numAfterCreateEvents_CustomExpiryTTLDestroy,
        expireNumKeys * expectedVmsForEvents, true, 600000);

    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterCreateEvents_CustomNoExpiryTTLDestroy",
        ParRegExpirationBB.numAfterCreateEvents_CustomNoExpiryTTLDestroy,
        noExpireNumKeys * expectedVmsForEvents, true, 600000);

    ParRegExpirationBB.getBB().getSharedCounters().increment(
        ParRegExpirationBB.numPopulationTask_Completed);

    // Wait for remote caches to receive all destroy events
    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterDestroyEvents_CustomTTLDestroy",
        ParRegExpirationBB.numAfterDestroyEvents_CustomTTLDestroy,
        expireNumKeys * expectedVmsForEvents, true, 600000);

    // Check that all were destroyed

    Set keySet = aRegion.keySet();

    if (keySet.size() != noExpireNumKeys) {
      throw new TestException(
          "Expected the region size after expiration to be the number of non expiry keys put into region "
              + noExpireNumKeys + " but has " + aRegion.size());
    }
    else {
      Log.getLogWriter().info(
          "Got the expected size of region after expiration as "
              + noExpireNumKeys);
    }

    Iterator iterator = keySet.iterator();
    while (iterator.hasNext()) {
      String key = iterator.next().toString();
      if (key.startsWith("Expire_")) {
        throw new TestException("Unexpected key " + key + " in the region "
            + aRegion.getName() + " after expiration");
      }
    }

    ParRegExpirationBB.getBB().printSharedCounters();
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterCreateEvents_CustomExpiryTTLDestroy);
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterCreateEvents_CustomNoExpiryTTLDestroy);
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterDestroyEvents_CustomTTLDestroy);
  }

  /**
   * Controls the thread that has an expiration action set in the entry for
   * region TTLInval (Custom expiry)
   */
  public void customEntryTTLInval() {
    String regionName = regionNames[CustomTTLInvalIndex];
    Region aRegion = CacheHelper.getCache().getRegion(regionName);

    String expiryKeyPrefix = "Expire_" + regionName + "_";
    int expireNumKeys = populateRegion(aRegion, expiryKeyPrefix);
    Object expireKey = regionName + "_expireNumKeys";
    Log.getLogWriter().info(
        "Putting " + expireKey + ", " + expireNumKeys + " into ExpirationBB");
    ExpirationBB.getBB().getSharedMap().put(expireKey,
        new Integer(expireNumKeys));

    String noExpiryKeyPrefix = "NotExpire_" + regionName + "_";
    int noExpireNumKeys = populateRegion(aRegion, noExpiryKeyPrefix);
    Object noExpireKey = regionName + "_noExpireNumKeys";
    Log.getLogWriter().info(
        "Putting " + noExpireKey + ", " + noExpireNumKeys
            + " into ExpirationBB");
    ExpirationBB.getBB().getSharedMap().put(noExpireKey,
        new Integer(noExpireNumKeys));
    
    int expectedVmsForEvents = 1;

    if (TestConfig.tab()
        .booleanAt(ExpirPrms.isBridgeClientConfiguration, false)) {
      expectedVmsForEvents = TestConfig.tab().intAt(ExpirPrms.numClientVms) + 1;
    }

    // Wait for this VM to receive all create events
    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterCreateEvents_CustomExpiryTTLInvalidate",
        ParRegExpirationBB.numAfterCreateEvents_CustomExpiryTTLInvalidate,
        expireNumKeys * expectedVmsForEvents, true, 600000);

    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterCreateEvents_CustomNoExpiryTTLInvalidate",
        ParRegExpirationBB.numAfterCreateEvents_CustomNoExpiryTTLInvalidate,
        noExpireNumKeys * expectedVmsForEvents, true, 600000);

    ParRegExpirationBB.getBB().getSharedCounters().increment(
        ParRegExpirationBB.numPopulationTask_Completed);

    // Wait for this VM to receive all invalidate events
    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterInvalidateEvents_CustomTTLInvalidate",
        ParRegExpirationBB.numAfterInvalidateEvents_CustomTTLInvalidate,
        expireNumKeys * expectedVmsForEvents, true, 600000);

    Iterator it = aRegion.keySet().iterator();
    Object key;
    while (it.hasNext()) { // it's ok to do gets, since no other cache has a
      // value either
      key = it.next();
      try {
        Object value = aRegion.get(key);
        if ((value != null && key.toString().startsWith("Expire_"))) {
          throw new TestException("Found key " + key
              + " which is not supposed to be expired with non-null value "
              + value);
        }
        else if ((value == null && !key.toString().startsWith("Expire_"))) {
          throw new TestException("Found key " + key
              + " which is supposed to be expired with null value " + value);
        }
      }
      catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }

    ParRegExpirationBB.getBB().printSharedCounters();
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterCreateEvents_CustomExpiryTTLInvalidate);
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterCreateEvents_CustomNoExpiryTTLInvalidate);
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterInvalidateEvents_CustomTTLInvalidate);
  }

  /**
   * Controls the thread that has an expiration action set in the entry for
   * region IdleTOInval (custom expiry)
   */
  public void customEntryIdleTODestroy() {
    String regionName = regionNames[CustomIdleTODestroyIndex];
    Region aRegion = CacheHelper.getCache().getRegion(regionName);

    String expiryKeyPrefix = "Expire_" + regionName + "_";
    int expireNumKeys = populateRegion(aRegion, expiryKeyPrefix);
    Object expireKey = regionName + "_expireNumKeys";
    Log.getLogWriter().info(
        "Putting " + expireKey + ", " + expireNumKeys + " into ExpirationBB");
    ExpirationBB.getBB().getSharedMap().put(expireKey,
        new Integer(expireNumKeys));

    String noExpiryKeyPrefix = "NotExpire_" + regionName + "_";
    int noExpireNumKeys = populateRegion(aRegion, noExpiryKeyPrefix);
    Object noExpireKey = regionName + "_noExpireNumKeys";
    Log.getLogWriter().info(
        "Putting " + noExpireKey + ", " + noExpireNumKeys
            + " into ExpirationBB");
    ExpirationBB.getBB().getSharedMap().put(noExpireKey,
        new Integer(noExpireNumKeys));

    
    int expectedVmsForEvents = 1;

    if (TestConfig.tab()
        .booleanAt(ExpirPrms.isBridgeClientConfiguration, false)) {
      expectedVmsForEvents = TestConfig.tab().intAt(ExpirPrms.numClientVms) + 1;
    }

    // Wait for this VM to receive all create events
    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterCreateEvents_CustomExpiryIdleTODestroy",
        ParRegExpirationBB.numAfterCreateEvents_CustomExpiryIdleTODestroy,
        expireNumKeys * expectedVmsForEvents, true, 600000);

    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterCreateEvents_CustomNoExpiryIdleTODestroy",
        ParRegExpirationBB.numAfterCreateEvents_CustomNoExpiryIdleTODestroy,
        noExpireNumKeys * expectedVmsForEvents, true, 600000);

    ParRegExpirationBB.getBB().getSharedCounters().increment(
        ParRegExpirationBB.numPopulationTask_Completed);

    // Wait for this VM to receive all invalidate events
    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterDestroyEvents_CustomIdleTODestroy",
        ParRegExpirationBB.numAfterDestroyEvents_CustomIdleTODestroy,
        expireNumKeys * expectedVmsForEvents, true, 600000);

    // Check that all entries with prefix Expire_ were destroyed

    Set keySet = aRegion.keySet();

    if (keySet.size() != noExpireNumKeys) {
      throw new TestException(
          "Expected the region size after expiration to be the number of non expiry keys put into region "
              + noExpireNumKeys + " but has " + aRegion.size());
    }
    else {
      Log.getLogWriter().info(
          "Got the expected size of region after expiration as "
              + noExpireNumKeys);
    }

    Iterator iterator = keySet.iterator();
    while (iterator.hasNext()) {
      String key = iterator.next().toString();
      if (key.startsWith("Expire_")) {
        throw new TestException("Unexpected key " + key + " in the region "
            + aRegion.getName() + " after expiration");
      }
    }

    ParRegExpirationBB.getBB().printSharedCounters();
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterCreateEvents_CustomExpiryIdleTODestroy);
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterCreateEvents_CustomNoExpiryIdleTODestroy);
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterDestroyEvents_CustomIdleTODestroy);
  }

  /**
   * Controls the thread that has an expiration action set in the entry for
   * region IdleTOInval (Custom expiry)
   */
  public void customEntryIdleTOInval() {
    String regionName = regionNames[CustomIdleTOInvalIndex];
    Region aRegion = CacheHelper.getCache().getRegion(regionName);

    String expiryKeyPrefix = "Expire_" + regionName + "_";
    int expireNumKeys = populateRegion(aRegion, expiryKeyPrefix);
    Object expireKey = regionName + "_expireNumKeys";
    Log.getLogWriter().info(
        "Putting " + expireKey + ", " + expireNumKeys + " into ExpirationBB");
    ExpirationBB.getBB().getSharedMap().put(expireKey,
        new Integer(expireNumKeys));

    String noExpiryKeyPrefix = "NotExpire_" + regionName + "_";
    int noExpireNumKeys = populateRegion(aRegion, noExpiryKeyPrefix);
    Object noExpireKey = regionName + "_noExpireNumKeys";
    Log.getLogWriter().info(
        "Putting " + noExpireKey + ", " + noExpireNumKeys
            + " into ExpirationBB");
    ExpirationBB.getBB().getSharedMap().put(noExpireKey,
        new Integer(noExpireNumKeys));

   int expectedVmsForEvents = 1;

    if (TestConfig.tab()
        .booleanAt(ExpirPrms.isBridgeClientConfiguration, false)) {
      expectedVmsForEvents = TestConfig.tab().intAt(ExpirPrms.numClientVms) + 1;
    }

    // Wait for this VM to receive all create events
    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterCreateEvents_CustomExpiryIdleTOInvalidate",
        ParRegExpirationBB.numAfterCreateEvents_CustomExpiryIdleTOInvalidate,
        expireNumKeys * expectedVmsForEvents, true, 600000);

    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterCreateEvents_CustomNoExpiryIdleTOInvalidate",
        ParRegExpirationBB.numAfterCreateEvents_CustomNoExpiryIdleTOInvalidate,
        noExpireNumKeys * expectedVmsForEvents, true, 600000);

    ParRegExpirationBB.getBB().getSharedCounters().increment(
        ParRegExpirationBB.numPopulationTask_Completed);

    // Wait for this VM to receive all invalidate events
    TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
        "numAfterInvalidateEvents_CustomIdleTOInvalidate",
        ParRegExpirationBB.numAfterInvalidateEvents_CustomIdleTOInvalidate,
        expireNumKeys * expectedVmsForEvents, true, 600000);

    Iterator it = aRegion.keySet().iterator();
    Object key;
    while (it.hasNext()) { // it's ok to do gets, since no other cache has a
      // value either
      key = it.next();
      try {
        Object value = aRegion.get(key);
        if ((value != null && key.toString().startsWith("Expire_"))) {
          throw new TestException("Found key " + key
              + " which is not supposed to be expired with non-null value "
              + value);
        }
        else if ((value == null && !key.toString().startsWith("Expire_"))) {
          throw new TestException("Found key " + key
              + " which is supposed to be expired with null value " + value);
        }
      }
      catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }

    ParRegExpirationBB.getBB().printSharedCounters();
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterCreateEvents_CustomExpiryIdleTOInvalidate);
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterCreateEvents_CustomNoExpiryIdleTOInvalidate);
    ParRegExpirationBB.getBB().getSharedCounters().zero(
        ParRegExpirationBB.numAfterInvalidateEvents_CustomIdleTOInvalidate);
  }

  /**
   * Populate a region with keys and values for a time beyond the given time.
   * 
   * @param aRegion -
   *                the region to populate
   * 
   * @returns - the number of keys/values put in the region.
   */
  protected int populateRegion(Region aRegion, String keyPrefix) {
    String regionName = aRegion.getName();
    Log.getLogWriter().info(
        "Populating region " + regionName + ", aRegion is "
            + TestHelper.regionToString(aRegion, true));

    // put for a time period
    long minTaskGranularitySec = TestConfig.tab().longAt(
        TestHelperPrms.minTaskGranularitySec);
    long msToRun = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    Log.getLogWriter().info("Putting for " + msToRun + " ms");
    int numKeys = 0;
    long startTimeMs = System.currentTimeMillis();
    do {
      String key = keyPrefix + (++numKeys);
      Object value = new Long(numKeys);
      try {
        Log.getLogWriter().info(
            "Putting key " + key + ", value " + value + " into region "
                + regionName);
        if (TestConfig.tab().booleanAt(ExpirPrms.useTransactions, false))
          TxHelper.begin();
        long putStart = System.currentTimeMillis();
        aRegion.put(key, value);
        long putEnd = System.currentTimeMillis();
        long duration = putEnd - putStart;
        Log.getLogWriter().info(
            "Done putting key " + key + ", value " + value + " into region "
                + regionName + ", put took " + duration + " ms");
        if (duration > 30000)
          throw new TestException("Put took a suspiciously long time, "
              + duration + " ms");
      }
      catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      catch (CacheWriterException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      finally {
        if (TxHelper.exists())
          TxHelper.commitExpectSuccess();
      }
    } while (System.currentTimeMillis() - startTimeMs < msToRun);
    Log.getLogWriter().info(
        "Done putting into " + regionName + " for " + msToRun + " ms, put "
            + numKeys + " keys/values");
    return numKeys;
  }

  /**
   * Kill the clients and bring it back again immediately
   * 
   */

  public static void killClient() {
    try {
      TestHelper.waitForCounter(ParRegExpirationBB.getBB(),
          "numPopulationTask_Completed",
          ParRegExpirationBB.numPopulationTask_Completed, 8, true, 600000);
      ClientVmInfo info = ClientVmMgr.stop("Killing the VM",
          ClientVmMgr.NICE_KILL, ClientVmMgr.IMMEDIATE);
    }
    catch (ClientVmNotFoundException e) {
      Log.getLogWriter().warning(" Exception while killing client ", e);
    }
  }
}
