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
package compression;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.Log;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.StopSchedulingOrder;
import hydra.TestConfig;
import hydra.blackboard.SharedMap;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import objects.Order;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;
import util.ValueHolder;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.LocalRegion;

import diskRecovery.RecoveryTestVersionHelper;

/**
 * The SerialCompressionTest class... </p>
 *
 * @author mpriest
 * @see ?
 * @since 7.x
 */
public class SerialCompressionTest extends CompressionTest {

  private static final String keyList_Key = "keyList_Key";
  private static final String parentRegionName = "parent";
  private static final String[] serverRegionNames       = { "repServer", "perRepServer", "partServer0",   "partServer1",   "perPartServer0",   "perPartServer1" };
  private static final String[] emptyRegionNames        = { "repEmpty" , "perRepEmpty",  "partAccessor0", "partAccessor1", "perPartAccessor0", "perPartAccessor1" };
  private static final String[] compressedRegionNames   = { "repComp"  , "perRepComp",   "partComp0",     "partComp1",     "perPartComp0",     "perPartComp1" };
  private static final String[] unCompressedRegionNames = { "repUnComp", "perRepUnComp", "partUnComp0",   "partUnComp1",   "perPartUnComp0",   "perPartUnComp1" };
  private int nbrThreadsInClients;
  private SharedMap sharedMap;
  private ArrayList<String> keyList;

  /**
   *
   */
  public synchronized static void HydraTask_initialize_Server() {
    if (testInstance == null) {
      testInstance = new SerialCompressionTest();
      testInstance.initialize();

      InitializeCache();
      BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
      InitializeRegions(serverRegionNames);
    }
  }

  /**
   *
   */
  public synchronized static void HydraTask_initialize_Client() {
    if (testInstance == null) {
      testInstance = new SerialCompressionTest();
      testInstance.initialize();

      InitializeCache();
      InitializeRegions(DeterminRegionNames());

      for (Region aRegion : testInstance.theRegions) {
        aRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
      }
    }
  }

  /**
   *
   */
  public synchronized static void HydraTask_initialize_Peer() {
    if (testInstance == null) {
      testInstance = new SerialCompressionTest();
      testInstance.initialize();

      InitializeCache();
      InitializeRegions(DeterminRegionNames());
    }
  }

  public synchronized static void HydraTask_initialize_Peer_Sub() {
    if (testInstance == null) {
      testInstance = new SerialCompressionTest();
      testInstance.initialize();

      InitializeCache();
      InitializeSubRegions();
    }
  }

  /**
   *
   */
  public static void HydraTask_doSerialRndRbnOps() {
    Log.getLogWriter().info("SerialCompressionTest.HydraTask_doSerialRndRbnOps.");
    ((SerialCompressionTest) testInstance).doSerialRndRbnOps();
  }

  /**
   *
   */
  public synchronized static void HydraTask_CompressionStats() {
    ((SerialCompressionTest) testInstance).compressionStats();
  }

  /**
   *
   */
  private synchronized static void InitializeCache() {
    if (CacheHelper.getCache() == null) {
      CacheHelper.createCache(ConfigPrms.getCacheConfig());
    }
  }

  /**
   *
   * @param regionNames
   */
  private synchronized static void InitializeRegions(String[] regionNames) {
    for (String regionName : regionNames) {
      Region aRegion = RegionHelper.createRegion(regionName);
      testInstance.theRegions.add(aRegion);
      Log.getLogWriter().info("SerialCompressionTest.InitializeRegions regionName=" + aRegion.getName() +
                              " Compressor=" + aRegion.getAttributes().getCompressor().getClass().getName());
    }

    testInstance.logRegionHierarchy();
  }

  private synchronized static void InitializeSubRegions() {
    Region parentRegion = RegionHelper.createRegion(parentRegionName);

    Region subRegion;
    String[] subRegionNames = DeterminRegionNames();
    for (String subRegionName : subRegionNames) {
      AttributesFactory attributesFactory = RegionHelper.getAttributesFactory(subRegionName);
      RegionAttributes regionAttributes = attributesFactory.createRegionAttributes();
      RecoveryTestVersionHelper.createDiskStore(regionAttributes);
      subRegion = parentRegion.createSubregion(subRegionName, regionAttributes);
      testInstance.theRegions.add(subRegion);
    }

    testInstance.logRegionHierarchy();
  }

  /**
   *
   * @return
   */
  private synchronized static String[] DeterminRegionNames() {

    String[] regionNames = compressedRegionNames;
    if (ShouldBeUncommpressed()) {
      regionNames = unCompressedRegionNames;
    } else if (ShouldBeEmpty()) {
      regionNames = emptyRegionNames;
    }

    return regionNames;
  }

  /**
   *
   * @return
   */
  private synchronized static boolean ShouldBeUncommpressed() {
    boolean shouldBeUncommpressed = false;
    long cntOfCompressedRegions = CompressionBB.getBB().getSharedCounters().read(CompressionBB.CntOfCompressedRegions);
    int desiredNbrOfCompressedRegions = CompressionPrms.getNbrOfCompressedRegions();
    if (cntOfCompressedRegions < desiredNbrOfCompressedRegions) {
      shouldBeUncommpressed = true;
      CompressionBB.getBB().getSharedCounters().increment(CompressionBB.CntOfCompressedRegions);
    }
    return shouldBeUncommpressed;
  }

  /**
   *
   * @return
   */
  private synchronized static boolean ShouldBeEmpty() {
    boolean shouldBeEmpty = false;
    long cntOfEmptyAccessors = CompressionBB.getBB().getSharedCounters().read(CompressionBB.CntOfEmptyAccessors);
    int desiredNbrOfEmptyAccessors = CompressionPrms.getNbrOfEmptyAccessors();
    if (cntOfEmptyAccessors < desiredNbrOfEmptyAccessors) {
      shouldBeEmpty = true;
      CompressionBB.getBB().getSharedCounters().increment(CompressionBB.CntOfEmptyAccessors);
    }
    return shouldBeEmpty;
  }

  protected void initialize() {
    super.initialize();
  }

  /**
   *
   */
  private void doSerialRndRbnOps() {
    Log.getLogWriter().info("SerialCompressionTest.doSerialRndRbnOps.");

    // the total number of working client threads in this test
    nbrThreadsInClients = RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads();
    int addPosition = 1;
    int updatePosition = 3;
    int resetPosition = nbrThreadsInClients;
    int maxNbrOfRounds = nbrThreadsInClients;

    long roundPosition = CompressionBB.getBB().getSharedCounters().incrementAndRead(CompressionBB.RoundPosition);
    Log.getLogWriter().info("SerialCompressionTest.doSerialRndRbnOps roundPosition=" + roundPosition);

    if (roundPosition == resetPosition) { // Reset (last) position in round, then Verify ops and Reset
      Log.getLogWriter().info("SerialCompressionTest.doSerialRndRbnOps we are in the RESET position in the round!");

      verifyOps(); // Verify the previous operations

      // now become the first position in the round
      CompressionBB.getBB().getSharedCounters().zero(CompressionBB.RoundPosition);
      roundPosition = CompressionBB.getBB().getSharedCounters().incrementAndRead(CompressionBB.RoundPosition);
    }

    if (roundPosition == addPosition) { // Add (first) position in round, then Do add ops
      // Increment and Read the round counter
      long roundCounter = CompressionBB.getBB().getSharedCounters().incrementAndRead(CompressionBB.RoundCounter);
      Log.getLogWriter().info("SerialCompressionTest.doSerialRndRbnOps we are in the ADD position in the round! roundCounter=" + roundCounter);

      if (roundCounter > maxNbrOfRounds) { // We have reached the maximum number of rounds, Stop the test
        Log.getLogWriter().info("MDP9-Success! We are stopping the test!");
        throw new StopSchedulingOrder("Stopping client.");
      }

      String keyPrefix = new Long(roundCounter).toString();
      doAddOps(keyPrefix);  // Perform some add operations

    } else if (roundPosition == updatePosition) { // Update (third) position in round, do Update Ops
      Log.getLogWriter().info("SerialCompressionTest.doSerialRndRbnOps we are in the UPDATE position in the round!");
      doUpdateOps(); // Perform update operations
      verifyOps();   // Verify the previous operations

    } else {
      Log.getLogWriter().info("SerialCompressionTest.doSerialRndRbnOps we must be in a VERIFY position in the round!");
      verifyOps();   // Verify the previous operations
    }

  }

  /**
   *
   * @param keyPrefix
   */
  private void doAddOps(String keyPrefix) {
    Log.getLogWriter().info("MDP-SerialCompressionTest.doAddOps round " + keyPrefix + " of " + nbrThreadsInClients);

    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    long startTime = System.currentTimeMillis();

    sharedMap = CompressionBB.getBB().getSharedMap();
    keyList = (ArrayList<String>) sharedMap.get(keyList_Key);
    if (keyList == null) {
      keyList = new ArrayList<String>();
    }

    // Enter a loop to generate random values based on the configuration defined in conf files
    int addCntr = 0;
    do {
      // Generate a new ValueHolder with random values and Store the Data
      storeOps(keyPrefix, ++addCntr, new ValueHolder(randomValues));
    } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);

    // Generate a Custom Object (Order)
    Order order = new Order();
    order.init(1);
    // Store the Data
    storeOps(keyPrefix, ++addCntr, order);

    // Save the List of Keys
    sharedMap.put(keyList_Key, keyList);
    Log.getLogWriter().info("MDP-SerialCompressionTest.doAddOps addCntr=" + addCntr);
    CompressionBB.getBB().getSharedCounters().add(CompressionBB.ObjectsAddedCnt, addCntr);

  }

  /**
   *
   */
  private void doUpdateOps() {
    Log.getLogWriter().info("MDP-SerialCompressionTest.doUpdateOps.");

    sharedMap = CompressionBB.getBB().getSharedMap();
    keyList = (ArrayList<String>) sharedMap.get(keyList_Key);
    if (keyList == null) {
      keyList = new ArrayList<String>();
    }

    for (Iterator it = keyList.iterator();it.hasNext();) {
      String key = (String) it.next();
      Object value;
      if (sharedMap.get(key) instanceof ValueHolder) {
        // Generate a new ValueHolder with random values
        value = new ValueHolder(randomValues);
      } else {
        // Generate a Custom Object (Order)
        Order order = new Order();
        order.init(2);
        value = order;
      }
      // Store the Data
      storeOps(key, value);
    }
  }

  /**
   * This is to be called to 'add' data to the region and the BlackBoard.  It first creates a key and stores it
   *   to a collection of created keys.  It then stores the value to the region(s) and the BlackBoard.
   * @param keyPrefix - the beginning part of the key to be created
   * @param keySuffix - the ending part of the key to be created
   * @param value     - the value to be stored in the BlackBoard and the region(s)
   */
  private void storeOps(String keyPrefix, int keySuffix, Object value) {
    // Create a new key
    String addKey = keyPrefix + "-" + keySuffix;
    // Store the key to the List of Keys
    keyList.add(addKey);
    storeOps(addKey, value);
  }

  /**
   * This is to be called to put data to the region and the BlackBoard.  It stores the value to the region(s) and
   *   the BlackBoard.
   * @param key   - the key to be stored in the BlackBoard and the region(s)
   * @param value - the value to be stored in the BlackBoard and the region(s)
   */
  private void storeOps(String key, Object value) {
    if (key.equals("1-1")){
      Log.getLogWriter().info("MDP-SerialCompressionTest.storeOps key=" + key + " value=" + value);
    }
    // Store the key / value pair to the BB for later comparison
    sharedMap.put(key, value);
    // Put the key / value pair to the Region
    for (Region aRegion : theRegions) {
      aRegion.put(key, value);
    }
  }

  /**
   *
   */
  private void verifyOps() {
    for (Region aRegion : theRegions) {
      verifyOps(aRegion);
    }
  }

  /**
   *
   * @param aRegion
   */
  private void verifyOps(Region aRegion) {
    Log.getLogWriter().info("SerialCompressionTest.verifyOps aRegion.getName()=" + aRegion.getName());

    sharedMap = CompressionBB.getBB().getSharedMap();
    keyList = (ArrayList<String>) sharedMap.get(keyList_Key);
    if (keyList == null) {
      throw new TestException("MDP-The keyList should NOT be NULL!");
    }
    Log.getLogWriter().info("SerialCompressionTest.verifyOps keyList.size()=" + keyList.size());

    int verifyCntr = 0;
    Object regionValue;
    Object bbValue;
    for (String key : keyList) {
      verifyCntr++;

      regionValue = aRegion.get(key);
      if (regionValue instanceof ValueHolder) {
        regionValue = ((ValueHolder) regionValue).getMyValue();
      }
      bbValue = sharedMap.get(key);
      if (bbValue instanceof ValueHolder) {
        bbValue = ((ValueHolder) bbValue).getMyValue();
      }

      if (regionValue.getClass().isArray() && bbValue.getClass().isArray()) {
        verifyArray(key, regionValue, bbValue);
      } else if (regionValue instanceof Map && bbValue instanceof Map) {
        verifyMap(key, regionValue, bbValue);
      } else if ((regionValue instanceof List && bbValue instanceof List)
                 || (regionValue instanceof Set && bbValue instanceof Set)) {
        verifyListOrSet(key, regionValue, bbValue);
      } else {
        verifyValues(key, regionValue, bbValue);
      }
    }
    Log.getLogWriter().info("MDP-SerialCompressionTest.verifyOps verifyCntr=" + verifyCntr);
    long addedCnt = CompressionBB.getBB().getSharedCounters().read(CompressionBB.ObjectsAddedCnt);
    if (verifyCntr < addedCnt) {
      throw new TestException("MDP9-Fail! The number of objects verified does not equal the number of objects added to the region.");
    }
  }

  /**
   *
   * @param key
   * @param valueFromRegion
   * @param valueFromBB
   */
  private void verifyMap(String key, Object valueFromRegion, Object valueFromBB) {
    Object regionMapValue;
    Object bbMapValue;
    Map regionValueMap = (Map) valueFromRegion;
    Map bbValueMap = (Map) valueFromBB;
    for (Object aMapKey : regionValueMap.keySet()) {
      regionMapValue = regionValueMap.get(aMapKey);
      bbMapValue = bbValueMap.get(aMapKey);
      if (regionMapValue.getClass().isArray() && bbMapValue.getClass().isArray()) {
        verifyArray(key, regionMapValue, bbMapValue);
      } else {
        verifyValues(key, regionMapValue, bbMapValue);
      }
    }
  }

  /**
   *
   * @param key
   * @param valueFromRegion
   * @param valueFromBB
   */
  private void verifyListOrSet(String key, Object valueFromRegion, Object valueFromBB) {
    Object[] regionValues = null;
    Object[] bbValues = null;
    if (valueFromRegion instanceof List && valueFromBB instanceof List) {
      regionValues = ((List) valueFromRegion).toArray();
      bbValues = ((List) valueFromBB).toArray();
    } else if (valueFromRegion instanceof Set && valueFromBB instanceof Set) {
      regionValues = ((Set) valueFromRegion).toArray();
      bbValues = ((Set) valueFromBB).toArray();
    }
    if (regionValues != null && bbValues != null) {
      for (int i = 0;i < regionValues.length;i++) {
        Object regionValue = regionValues[i];
        Object bbValue = bbValues[i];
        if (regionValue.getClass().isArray() && bbValue.getClass().isArray()) {
          verifyArray(key, regionValue, bbValue);
        } else {
          verifyValues(key, regionValue, bbValue);
        }
      }
    }
  }

  /**
   *
   * @param key
   * @param valueFromRegion
   * @param valueFromBB
   */
  private void verifyArray(String key, Object valueFromRegion, Object valueFromBB) {
    Object regionArrayValue = null;
    Object bbArrayValue = null;
    String regionClassType = valueFromRegion.getClass().getSimpleName();
    int arrayLength = Array.getLength(valueFromRegion);
    for (int i = 0;i < arrayLength;i++) {
      if (regionClassType.contains("boolean")) {
        regionArrayValue = Array.getBoolean(valueFromRegion, i);
        bbArrayValue = Array.getBoolean(valueFromBB, i);
      } else if (regionClassType.contains("byte")) {
        regionArrayValue = Array.getByte(valueFromRegion, i);
        bbArrayValue = Array.getByte(valueFromBB, i);
      } else if (regionClassType.contains("char")) {
        regionArrayValue = Array.getChar(valueFromRegion, i);
        bbArrayValue = Array.getChar(valueFromBB, i);
      } else if (regionClassType.contains("double")) {
        regionArrayValue = Array.getDouble(valueFromRegion, i);
        bbArrayValue = Array.getDouble(valueFromBB, i);
      } else if (regionClassType.contains("float")) {
        regionArrayValue = Array.getFloat(valueFromRegion, i);
        bbArrayValue = Array.getFloat(valueFromBB, i);
      } else if (regionClassType.contains("int")) {
        regionArrayValue = Array.getInt(valueFromRegion, i);
        bbArrayValue = Array.getInt(valueFromBB, i);
      } else if (regionClassType.contains("long")) {
        regionArrayValue = Array.getLong(valueFromRegion, i);
        bbArrayValue = Array.getLong(valueFromBB, i);
      } else if (regionClassType.contains("short")) {
        regionArrayValue = Array.getShort(valueFromRegion, i);
        bbArrayValue = Array.getShort(valueFromBB, i);
      } else {
        Log.getLogWriter().info("MDP-SerialCompressionTest.verifyArray regionClassType=" + regionClassType +
                                " arrayLength=" + arrayLength);
      }

      verifyValues(key, regionArrayValue, bbArrayValue);
    }
  }

  /**
   * This is used to verify that the value retrieved from the region are equal to the value retrieved from the
   * BlackBoard Throws a TestException to indicate failure.
   *
   * @param key             - the String that was used to retrieve the objects
   * @param valueFromRegion - the Object that was retrived from the Region
   * @param valueFromBB     - the Object that was retrived from the BlackBoard
   */
  private void verifyValues(String key, Object valueFromRegion, Object valueFromBB) {
    // Check the object types to see if we need to get the String values
    if ((valueFromRegion instanceof StringBuffer && valueFromBB instanceof StringBuffer)
        || (valueFromRegion instanceof Order && valueFromBB instanceof Order)) {
      valueFromRegion = valueFromRegion.toString();
      valueFromBB = valueFromBB.toString();
    }
    // Check for equality and throw exception when not equal
    if (!valueFromRegion.equals(valueFromBB)) {
      throw new TestException("MDP9-Fail! The values are not equal! key=" + key +
                              " valueFromRegion=" + valueFromRegion +
                              " valueFromBB=" + valueFromBB);
    }
  }

  /**
   *
   */
  private void compressionStats() {
    StringBuilder errorString = new StringBuilder();
    String regionName;
    Compressor compressor;
    CachePerfStats regionPerfStats;
    DataPolicy dataPolicy;
    long totalPreCompressedBytes;
    long totalCompressionTime;
    long totalCompressions;
    long totalPostCompressedBytes;
    long totalDecompressionTime;
    long totalDecompressions;
    for (Region aRegion : theRegions) {
      regionName = aRegion.getName();
      Log.getLogWriter().info("MDP-S-SerialCompressionTest.stats regionName = " + regionName);

      RegionAttributes attributes = aRegion.getAttributes();
      compressor = attributes.getCompressor();
      Log.getLogWriter().info("MDP-S-  Compressor=" + (compressor == null ? null : compressor.getClass().getName()));

      regionPerfStats = ((LocalRegion) aRegion).getRegionPerfStats();
      if (regionPerfStats == null) {
        continue;
      }
      totalPreCompressedBytes = regionPerfStats.getTotalPreCompressedBytes();
      totalCompressionTime = regionPerfStats.getTotalCompressionTime();
      totalCompressions = regionPerfStats.getTotalCompressions();
      totalPostCompressedBytes = regionPerfStats.getTotalPostCompressedBytes();
      totalDecompressionTime = regionPerfStats.getTotalDecompressionTime();
      totalDecompressions = regionPerfStats.getTotalDecompressions();
      Log.getLogWriter().info("\n        totalPreCompressedBytes   = " + totalPreCompressedBytes +
                              "\n        totalCompressionTime   = " + totalCompressionTime/1000000000d + " (sec)" +
                              "\nMDP-S-  totalCompressions      = " + totalCompressions +
                              "\n        totalPostCompressedBytes = " + totalPostCompressedBytes +
                              "\n        totalDecompressionTime = " + totalDecompressionTime/1000000000d + " (sec)" +
                              "\n        totalDecompressions    = " + totalDecompressions);

      dataPolicy = attributes.getDataPolicy();
      Log.getLogWriter().info("MDP-S-SerialCompressionTest.stats dataPolicy=" + dataPolicy);
      if (dataPolicy.equals(DataPolicy.EMPTY) || compressor == null) {
        if (totalCompressions != 0) {
          errorString.append("The total number of compressions for the region '" + regionName + "' should be zero.\n");
        }
        if (totalPreCompressedBytes != 0) {
          errorString.append("The total number of pre-compressed bytes for the region '" + regionName + "' should be zero.\n");
        }
        if (totalCompressionTime != 0) {
          errorString.append("The total compression time for the region '" + regionName + "' should be zero.\n");
        }
        if (totalDecompressions != 0) {
          errorString.append("The total number of decompressions for the region '" + regionName + "' should be zero.\n");
        }
        if (totalPostCompressedBytes != 0) {
          errorString.append("The total number of post-decompressed bytes for the region '" + regionName + "' should be zero.\n");
        }
        if (totalDecompressionTime != 0) {
          errorString.append("The total decompression time for the region '" + regionName + "' should be zero.\n");
        }
      } else {
        if (totalCompressions <= 0) {
          errorString.append("The total number of compressions for the region '" + regionName + "' should be greater than zero.\n");
        }
        if (totalPreCompressedBytes <= 0) {
          errorString.append("The total number of pre-compressed bytes for the region '" + regionName + "' should be greater than zero.\n");
        }
        if (totalCompressionTime <= 0) {
          errorString.append("The total compression time for the region '" + regionName + "' should be greater than zero.\n");
        }
        if (totalDecompressions <= 0) {
          errorString.append("The total number of decompressions for the region '" + regionName + "' should be greater than zero.\n");
        }
        if (totalPostCompressedBytes <= 0) {
          errorString.append("The total number of post-decompressed bytes for the region '" + regionName + "' should be greater than zero.\n");
        }
        if (totalDecompressionTime <= 0) {
          errorString.append("The total decompression time for the region '" + regionName + "' should be greater than zero.\n");
        }
      }
    }
    if (errorString.length() > 0) {
      throw new TestException("MDP9-Fail! There is a problem with the compression statistics.\n" + errorString.toString());
    }
  }

}
