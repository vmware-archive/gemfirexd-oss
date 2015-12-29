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
package parReg.execute.useCase1;

import hydra.CacheHelper;
import hydra.Log;
import hydra.RegionHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import parReg.ParRegBB;
import parReg.execute.HashMapResultCollector;
import parReg.execute.OnRegionsFunction;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.internal.cache.BucketDump;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.execute.InternalFunctionService;

public class UseCase1 {

  static protected UseCase1 testInstance;

  private static Cache theCache;
  
  public static Region rootRegion;

  public static Region fxRateRegion;

  public static Region hierarchyRegion;

  public static Region industrySectorRegion;

  public static PartitionedRegion positionRegion; // they are colocated

  public static PartitionedRegion instrumentRegion; // they are colocated

  public static PartitionedRegion sensitivityRegion; // they are colocated

  public static PartitionedRegion undSensitivityRegion; // they are colocated

  public static PartitionedRegion positionRiskRegion;

  public static PartitionedRegion positionUndRiskRegion;

  public static UseCase1Function useCase1Function = new UseCase1Function();

  // nm_region
  public static final String LON = "LON";

  public static final String NYC = "NYC";

  public static final String TOK = "TOK";

  public static final String[] region = { LON, NYC, TOK };

  // id_param_rsk values
  public static final String DELT = "DELT";

  public static final String VEGA = "VEGA";

  public static final String GAMM = "GAMM";

  public static final String[] param_rsk = { DELT, VEGA, GAMM };

  // id_type_imnt values
  public static final String STK = "STK";

  public static final String XFX = "XFX";

  public static final String[] type_imnt = { STK, XFX };

  // id_ccy_unit
  public static final String USD = "USD";

  public static final String EUR = "EUR";

  public static final String[] ccy_unit = { USD, EUR };

  private static ArrayList errorMsgs = new ArrayList();

  private static ArrayList errorException = new ArrayList();

  private static ArrayList bucketsInTheDataStore;

  private static String bucketReferenceRegion;

  private static ArrayList primaryBucketsInTheDataStore;

  private static String primaryBucketReferenceRegion;
  
  Set regionSet = new HashSet();

  public synchronized static void HydraTask_p2p_dataStoreInitialize() {
    if (testInstance == null) {
      testInstance = new UseCase1();
      ((UseCase1)testInstance).initializeDataStore();
    }
  }

  public synchronized static void HydraTask_p2p_accessorInitialize() {
    if (testInstance == null) {
      testInstance = new UseCase1();
      ((UseCase1)testInstance).initializeAccessor();
    }
  }

  public static Region getFxRateRegion() {
    return fxRateRegion;
  }

  public static Region getHierarchyRegion() {
    return hierarchyRegion;
  }

  public static PartitionedRegion getPositionRiskRegion() {
    return positionRiskRegion;
  }

  public static PartitionedRegion getInstrumentSensitivityRegion() {
    return sensitivityRegion;
  }

  public static PartitionedRegion getPositionUndRiskRegion() {
    return positionUndRiskRegion;
  }

  public static PartitionedRegion getUndSensitivityRiskRegion() {
    return undSensitivityRegion;
  }

  public static PartitionedRegion getInstrumentRegion() {
    return instrumentRegion;
  }

  public static String getRandomStringFromArray(String[] array) {
    Random generator = new Random();
    int rnd = generator.nextInt(array.length);
    return array[rnd];
  }

  public static void initializeDataStore() {
    theCache = CacheHelper.createCache("cache1");
    testInstance.createReplicatedRegions();
    testInstance.createDataStorePRs();
    FunctionService.registerFunction(useCase1Function);
  }

  public static void initializeAccessor() {
    theCache = CacheHelper.createCache("cache1");
    testInstance.createReplicatedRegions();
    testInstance.createAccessorPRs();
    FunctionService.registerFunction(useCase1Function);
  }

  private void createReplicatedRegions() {
    rootRegion = RegionHelper.createRegion("rootRegion");
    regionSet.add(rootRegion);
    fxRateRegion = RegionHelper.createRegion("fxRateRegion");
    regionSet.add(fxRateRegion);
    hierarchyRegion = RegionHelper.createRegion("hierarchyRegion");
    regionSet.add(hierarchyRegion);
    industrySectorRegion = RegionHelper.createRegion("industrySectorRegion");
    regionSet.add(industrySectorRegion);
  }

  private void createDataStorePRs() {
    instrumentRegion = (PartitionedRegion)RegionHelper
        .createRegion("instrumentRegion");
    regionSet.add(instrumentRegion);
    positionRegion = (PartitionedRegion)rootRegion.createSubregion(
        "positionRegion", RegionHelper.getRegionAttributes("positionRegion"));
    regionSet.add(positionRegion);
    sensitivityRegion = (PartitionedRegion)RegionHelper
        .createRegion("sensitivityRegion");
    regionSet.add(sensitivityRegion);
    undSensitivityRegion = (PartitionedRegion)RegionHelper
        .createRegion("undSensitivityRegion");
    regionSet.add(undSensitivityRegion);
    positionRiskRegion = (PartitionedRegion)RegionHelper
        .createRegion("positionRiskRegion");
    regionSet.add(positionRiskRegion);
    positionUndRiskRegion = (PartitionedRegion)RegionHelper
        .createRegion("positionUndRiskRegion");
    regionSet.add(positionUndRiskRegion);
  }

  private void createAccessorPRs() {
    instrumentRegion = (PartitionedRegion)RegionHelper
        .createRegion("accessorInstrumentRegion");
    regionSet.add(instrumentRegion);
    positionRegion = (PartitionedRegion)rootRegion.createSubregion(
        "positionRegion", RegionHelper
            .getRegionAttributes("accessorPositionRegion"));
    regionSet.add(positionRegion);
    sensitivityRegion = (PartitionedRegion)RegionHelper
        .createRegion("accessorSensitivityRegion");
    regionSet.add(sensitivityRegion);
    undSensitivityRegion = (PartitionedRegion)RegionHelper
        .createRegion("accessorUndSensitivityRegion");
    regionSet.add(undSensitivityRegion);
    positionRiskRegion = (PartitionedRegion)RegionHelper
        .createRegion("accessorPositionRiskRegion");
    regionSet.add(positionRiskRegion);
    positionUndRiskRegion = (PartitionedRegion)RegionHelper
        .createRegion("accessorPositionUndRiskRegion");
    regionSet.add(positionUndRiskRegion);
  }

  public static void HydraTask_populateRegions() {
    populateFxRateRegion();
    populateHierarchyRegion();
    populateIndustrySectorRegion();

    populateInstrumentRegion(); // populating the colocated PRs
    populateImntRiskSensRegion(); // populating the colocated PRs
    populateUndImntRiskSensRegion(); // populating the colocated PRs
    populatePositionRegion(); // populating the colocated PRs

    populatePositionRegion(); // varying the position
  }

  private static void populateFxRateRegion() {
    for (int i = 0; i < 500; i++) {
      String id_ccy_std = getRandomStringFromArray(ccy_unit);
      int id_region = 1;
      String id_ccy_bse = USD;
      float rt_fx;
      if (id_ccy_std.equals(USD)) {
        rt_fx = (float)1.2;
      }
      else {
        rt_fx = (float)0.5;
      }
      FxRate rate = new FxRate(id_ccy_std, id_region, id_ccy_bse, rt_fx);
      Log.getLogWriter().info("Putting the fx rate " + rate);
      fxRateRegion.put(rate.id_ccy_std + "|" + rate.id_ccy_bse, rate);
    }
  }

  private static void populateHierarchyRegion() {
    for (int i = 0; i < 500; i++) {
      int id_book = (new Random().nextInt(25));
      String nm_book = "Book " + new Integer(id_book).toString();
      int id_prtf = (new Random().nextInt(50));
      String nm_prtf = "Portfolio " + new Integer(id_prtf).toString();
      int id_country = (new Random().nextInt(125));
      String nm_country = "Country " + new Integer(id_country).toString();
      int id_desk = (new Random().nextInt(60));
      String nm_desk = "Desk " + new Integer(id_country).toString();
      int id_region = (new Random().nextInt(3));
      String nm_region = region[id_region];

      Hierarchy hierarchy = new Hierarchy(id_book, nm_book, id_prtf, nm_prtf,
          id_country, nm_country, id_desk, nm_desk, id_region, nm_region);
      Log.getLogWriter().info("Putting the hierarchy " + hierarchy);
      hierarchyRegion.put(String.valueOf(hierarchy.id_book), hierarchy);
    }
  }

  private static void populateIndustrySectorRegion() {
    for (int i = 0; i < 500; i++) {
      int id_sector = (new Random().nextInt(25));
      int id_parent = 100 + id_sector;
      String nm_sector = "NM_Sector: " + new Integer(id_sector).toString();
      String tx_sector = "TX_Sector: " + new Integer(id_sector).toString();
      IndustrySector sector = new IndustrySector(id_sector, id_parent,
          nm_sector, tx_sector);
      Log.getLogWriter().info("Putting the sector " + sector);
      industrySectorRegion.put(String.valueOf(sector.id_sector), sector);
    }
  }

  private static void populatePositionRegion() {
    for (int i = 0; i < 500; i++) {
      int id_posn_new = (new Random().nextInt(50));
      int id_imnt = (new Random().nextInt(10));
      int id_book = (new Random().nextInt(25));
      float am_posn = (new Random().nextFloat()) * id_posn_new;
      Position position = new Position(id_posn_new, id_imnt, id_book, am_posn);
      Log.getLogWriter().info("Putting the position " + position);
      Log.getLogWriter().info("Value is " + position.id_imnt);
      positionRegion.put(
          new RiskPartitionKey(new String(position.id_book + "|"
              + position.id_book), position.id_imnt,
              RiskPartitionKey.TYPE_POSITION), position);
    }
  }

  private static void populateInstrumentRegion() {
    for (int i = 0; i < 500; i++) {
      int id_imnt = (new Random().nextInt(10));
      String nm_imnt = "Instrument " + new Integer(id_imnt).toString();
      String id_typ_imnt = getRandomStringFromArray(type_imnt);
      String id_ccy_main = getRandomStringFromArray(ccy_unit);
      float am_sz_ctrt = 10000 * new Random().nextFloat();
      int id_sector = (new Random().nextInt(25));
      String id_ctry_issuer = "Country " + (new Random().nextInt(10));
      Instrument instrument = new Instrument(id_imnt, nm_imnt, id_typ_imnt,
          id_ccy_main, am_sz_ctrt, id_sector, id_ctry_issuer);
      Log.getLogWriter().info("Putting the instrument" + instrument);
      instrumentRegion.put(new RiskPartitionKey(String.valueOf(id_imnt),
          instrument.id_imnt, RiskPartitionKey.TYPE_INSTRUMENT), instrument);
    }
  }

  private static void populateImntRiskSensRegion() {
    for (int i = 0; i < 500; i++) {
      int id_imnt = (new Random().nextInt(10));
      String id_param_rsk = getRandomStringFromArray(param_rsk);
      float am_exp_rsk = (new Random().nextInt(100)) * new Random().nextFloat();
      String id_ccy_unit = getRandomStringFromArray(ccy_unit);

      ImntRiskSensitivity sensitivity = new ImntRiskSensitivity(id_imnt,
          id_param_rsk, am_exp_rsk, id_ccy_unit);
      Log.getLogWriter().info(
          "Putting the instrument sensitivity " + sensitivity);
      sensitivityRegion.put(new RiskPartitionKey(new String(sensitivity.id_imnt
          + "|" + sensitivity.id_param_rsk), sensitivity.id_imnt,
          RiskPartitionKey.TYPE_INSTRUMENT_RISK_SENSITIVITY), sensitivity);
    }
  }

  private static void populateUndImntRiskSensRegion() {
    for (int i = 0; i < 500; i++) {
      int id_imnt = (new Random().nextInt(10));
      int id_imnt_und = (new Random().nextInt(10)); // PositionUndRisk.d_imnt_und
      String id_param_rsk = getRandomStringFromArray(param_rsk);
      ;
      float am_exp_rsk = (new Random().nextInt(100)) * new Random().nextFloat();
      ;
      String id_ccy_unit = getRandomStringFromArray(ccy_unit);

      UndRiskSensitivity risk = new UndRiskSensitivity(id_imnt, id_imnt_und,
          id_param_rsk, am_exp_rsk, id_ccy_unit);
      Log.getLogWriter().info("Putting the und_Sensitivity " + risk);
      undSensitivityRegion.put(new RiskPartitionKey(risk.id_imnt + "|"
          + risk.id_imnt_und + "|" + risk.id_param_rsk, risk.id_imnt,
          RiskPartitionKey.TYPE_UND_RISK_SENSITIVITY), risk);
    }

  }

  public static void HydraTask_dumpPRBuckets() {
    Log.getLogWriter().info("Dumping positionRegion");
    positionRegion.dumpAllBuckets(false, Log.getLogWriter().convertToLogWriterI18n());
    Log.getLogWriter().info("Dumping instrumentRegion");
    instrumentRegion.dumpAllBuckets(false, Log.getLogWriter().convertToLogWriterI18n());
    Log.getLogWriter().info("Dumping sensitivityRegion");
    sensitivityRegion.dumpAllBuckets(false, Log.getLogWriter().convertToLogWriterI18n());
    Log.getLogWriter().info("Dumping undSensitivityRegion");
    undSensitivityRegion.dumpAllBuckets(false, Log.getLogWriter().convertToLogWriterI18n());
  }

  public static void HydraTask_logRegionSize() {
    testInstance.logRegionSize();
  }
  
  public static void HydraTask_invalidateEntries() {
    testInstance.invalidateEntries();
  }

  public static void HydraTask_verifyColocatedRegions() {
    HydraTask_verifyCustomPartitioning();
    HydraTask_verifyCoLocation();

    if (errorMsgs.size() != 0) {
      for (int index = 0; index < errorMsgs.size(); index++)
        Log.getLogWriter().error((String)errorMsgs.get(index));
    }
    if (errorException.size() != 0) {
      for (int index = 0; index < errorException.size(); index++)
        throw (TestException)errorException.get(index);
    }
  }

  private static void HydraTask_verifyCoLocation() {

    verifyPrimaryBucketCoLocation(instrumentRegion);
    verifyBucketCoLocation(instrumentRegion);

    verifyPrimaryBucketCoLocation(positionRegion);
    verifyBucketCoLocation(positionRegion);

    verifyPrimaryBucketCoLocation(sensitivityRegion);
    verifyBucketCoLocation(sensitivityRegion);

    verifyPrimaryBucketCoLocation(undSensitivityRegion);
    verifyBucketCoLocation(undSensitivityRegion);

  }
  
  protected void logRegionSize() {
    hydra.Log.getLogWriter().info("Passing the region set " + regionSet);
    Execution dataSet = InternalFunctionService.onRegions(regionSet).withArgs(
        "regionSize").withCollector(new HashMapResultCollector());
    HashMap map = null;

    try {
      map = (HashMap)dataSet.execute(new OnRegionsFunction()).getResult();
    }
    catch (Exception e) {
      throw new TestException("Caught Exception " , e);
    }
    hydra.Log.getLogWriter().info("Region sizes are " + map);
  }
  
  protected void invalidateEntries() {
    hydra.Log.getLogWriter().info("Passing the region set " + regionSet);
    Execution dataSet = InternalFunctionService.onRegions(regionSet).withArgs(
        "invalidateEntries");

    try {
      dataSet.execute(new OnRegionsFunction()).getResult();
    }
    catch (Exception e) {
      throw new TestException("Caught Exception " , e);
    }

    for (Object aRegion : regionSet) {
      Set keySet = ((Region)aRegion).keySet();
      for (Object key : keySet) {
        Object value = ((Region)aRegion).get(key);
        if (value != null) {
          throw new TestException(
              "Values supposed to be invalidated after clear Region, but key "
                  + key + " has value " + value);
        }
      }

    }
  }

  protected static void verifyPrimaryBucketCoLocation(PartitionedRegion aRegion) {

    String regionName = aRegion.getName();
    ArrayList primaryBucketList = (ArrayList)aRegion
        .getLocalPrimaryBucketsListTestOnly();
    Log.getLogWriter().info(
        "The primary buckets in this data Store for the Partioned Region "
            + regionName);

    if (primaryBucketList == null) {
      if (((PartitionedRegion)aRegion).getLocalMaxMemory() == 0) {
        Log.getLogWriter().info(
            "This is an accessor and no need to verify colocation");
        return;
      }
      else {
        throw new TestException(
            "Bucket List returned null, but it is not an accessor");
      }
    }

    Log.getLogWriter().info(
        "Primary Buckets of " + aRegion.getName() + " "
            + primaryBucketList.toString());

    if (primaryBucketsInTheDataStore == null) {
      Log
          .getLogWriter()
          .info(
              " Setting the reference primary buckets in the Data Store for this vm with the Partitioned Region "
                  + aRegion.getName());
      primaryBucketsInTheDataStore = primaryBucketList;
      primaryBucketReferenceRegion = regionName;
    }
    else {
      Log
          .getLogWriter()
          .info(
              "Reference primary buckets in the Data Store for this vm already set");
      Log.getLogWriter().info(" Verifying for the region " + regionName);

      Iterator iterator = primaryBucketList.iterator();

      while (iterator.hasNext()) {
        Integer currentPrimaryBucket = (Integer)iterator.next();
        if (primaryBucketsInTheDataStore.contains(currentPrimaryBucket)) {
          Log.getLogWriter().info(
              "Both the Regions " + primaryBucketReferenceRegion + " and "
                  + regionName + " have the bucket " + currentPrimaryBucket
                  + " in this node");
        }
        else {
          errorException.add(new TestException("Region " + regionName
              + " does not have its bucket " + currentPrimaryBucket
              + " colocated"));
          errorMsgs.add(("Region " + regionName + " does not have its bucket "
              + currentPrimaryBucket + " colocated"));
        }
      }

      Log.getLogWriter().info("Looking for missed buckets");

      iterator = primaryBucketsInTheDataStore.iterator();

      while (iterator.hasNext()) {
        Integer referenceRegionBucket = (Integer)iterator.next();
        if (primaryBucketList.contains(referenceRegionBucket)) {
          Log.getLogWriter().info(
              "Both the Regions " + bucketReferenceRegion + " and "
                  + regionName + " have the primary bucket "
                  + referenceRegionBucket + " in this node");
        }
        else {
          errorException.add(new TestException("Region " + regionName
              + " does not have its primary bucket " + referenceRegionBucket
              + " colocated"));
          errorMsgs
              .add(("Region " + regionName
                  + " does not have its primary bucket "
                  + referenceRegionBucket + " colocated"));
        }
      }
    }

  }

  protected static void verifyBucketCoLocation(PartitionedRegion aRegion) {
    String regionName = aRegion.getName();
    ArrayList bucketList = (ArrayList)aRegion.getLocalBucketsListTestOnly();
    Log.getLogWriter()
        .info(
            "The buckets in this data Store for the Partioned Region "
                + regionName);

    if (bucketList == null) {
      if (((PartitionedRegion)aRegion).getLocalMaxMemory() == 0) {
        Log.getLogWriter().info(
            "This is an accessor and no need to verify colocation");
        return;
      }
      else {
        throw new TestException(
            "Bucket List returned null, but it is not an accessor");
      }
    }

    Log.getLogWriter().info(
        "Buckets of " + aRegion.getName() + " " + bucketList.toString());
    if (bucketsInTheDataStore == null) {
      Log
          .getLogWriter()
          .info(
              " Setting the reference buckets in the Data Store for this vm with the Partitioned Region "
                  + aRegion.getName());
      bucketsInTheDataStore = bucketList;
      bucketReferenceRegion = regionName;
    }
    else {
      Log
          .getLogWriter()
          .info(
              "Reference primary buckets in the Data Store for this vm already set");
      Log.getLogWriter().info(" Verifying for the region " + regionName);

      Iterator iterator = bucketList.iterator();

      while (iterator.hasNext()) {
        Integer currentRegionBucket = (Integer)iterator.next();
        if (bucketsInTheDataStore.contains(currentRegionBucket)) {
          Log.getLogWriter().info(
              "Both the Regions " + bucketReferenceRegion + " and "
                  + regionName + " have the bucket " + currentRegionBucket
                  + " in this node");
        }
        else {
          errorException.add(new TestException("Region " + regionName
              + " does not have its bucket " + currentRegionBucket
              + " colocated"));
          errorMsgs.add(("Region " + regionName + " does not have its bucket "
              + currentRegionBucket + " colocated"));
        }
      }

      Log.getLogWriter().info("Looking for missed buckets");

      iterator = bucketsInTheDataStore.iterator();

      while (iterator.hasNext()) {
        Integer referenceRegionBucket = (Integer)iterator.next();
        if (bucketList.contains(referenceRegionBucket)) {
          Log.getLogWriter().info(
              "Both the Regions " + bucketReferenceRegion + " and "
                  + regionName + " have the bucket " + referenceRegionBucket
                  + " in this node");
        }
        else {
          errorException.add(new TestException("Region " + regionName
              + " does not have its bucket " + referenceRegionBucket
              + " colocated"));
          errorMsgs.add(("Region " + regionName + " does not have its bucket "
              + referenceRegionBucket + " colocated"));
        }
      }
    }
  }

  private static void HydraTask_verifyCustomPartitioning() {
    verifyCustomPartitioning(instrumentRegion);
    verifyCustomPartitioning(positionRegion);
    verifyCustomPartitioning(sensitivityRegion);
    verifyCustomPartitioning(undSensitivityRegion);
  }

  protected static void verifyCustomPartitioning(PartitionedRegion aRegion) {

    PartitionedRegion pr = (PartitionedRegion)aRegion;
    int totalBuckets = pr.getTotalNumberOfBuckets();
    RegionAttributes attr = aRegion.getAttributes();
    PartitionAttributes prAttr = attr.getPartitionAttributes();
    int redundantCopies = prAttr.getRedundantCopies();
    int expectedNumCopies = redundantCopies + 1;

    int verifyBucketCopiesBucketId = 0;

    while (true) {

      if (verifyBucketCopiesBucketId >= totalBuckets) {
        break; // we have verified all buckets
      }

      Log.getLogWriter().info(
          "Verifying data for bucket id " + verifyBucketCopiesBucketId
              + " out of " + totalBuckets + " buckets");
      List<BucketDump> listOfMaps = null;
      try {
        listOfMaps = pr.getAllBucketEntries(verifyBucketCopiesBucketId);
      }
      catch (ForceReattemptException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }

      // check that we have the correct number of copies of each bucket
      // listOfMaps could be size 0; this means we have no entries in this
      // particular bucket
      int size = listOfMaps.size();
      if (size == 0) {
        Log.getLogWriter().info(
            "Bucket " + verifyBucketCopiesBucketId + " is empty");
        verifyBucketCopiesBucketId++;
        continue;
      }

      if (size != expectedNumCopies) {
        errorMsgs
            .add(("For bucketId " + verifyBucketCopiesBucketId + ", expected "
                + expectedNumCopies + " bucket copies, but have " + listOfMaps
                .size()));
        errorException.add(new TestException("For bucketId "
            + verifyBucketCopiesBucketId + ", expected " + expectedNumCopies
            + " bucket copies, but have " + listOfMaps.size()));
      }
      else {
        Log.getLogWriter().info(
            "For bucketId " + verifyBucketCopiesBucketId + ", expected "
                + expectedNumCopies + " bucket copies, and have "
                + listOfMaps.size());
      }

      Log.getLogWriter().info(
          "Validating co-location for all the redundant copies of the bucket with Id : "
              + verifyBucketCopiesBucketId);
      // Check that all copies of the buckets have the same data
      for (int i = 0; i < listOfMaps.size(); i++) {
        BucketDump dump = listOfMaps.get(i);
        Map map = dump.getValues();
        verifyCustomPartition(map, verifyBucketCopiesBucketId);
        verifyUniqueBucketForCustomPartioning(verifyBucketCopiesBucketId);
      }

      verifyBucketCopiesBucketId++;
    }
  }

  protected static void verifyCustomPartition(Map map, int bucketid) {

    Iterator iterator = map.entrySet().iterator();
    Map.Entry entry = null;
    RiskPartitionKey key = null;

    while (iterator.hasNext()) {
      entry = (Map.Entry)iterator.next();
      key = (RiskPartitionKey)entry.getKey();

      if (ParRegBB.getBB().getSharedMap().get(
          "RoutingObjectForBucketid:" + bucketid) == null) {
        Log.getLogWriter().info(
            "RoutingObject for the bucket id to be set in the BB");
        ParRegBB.getBB().getSharedMap().put(
            "RoutingObjectForBucketid:" + bucketid,
            key.getInstrumentId().toString());
        ParRegBB.getBB().getSharedMap().put(
            "RoutingObjectKeyBucketid:" + bucketid, key);
        Log.getLogWriter().info(
            "BB value set to " + key.getInstrumentId().toString());
      }
      else {
        Log.getLogWriter().info("Checking the value for the routing object ");
        String blackBoardRoutingObject = (String)ParRegBB.getBB()
            .getSharedMap().get("RoutingObjectForBucketid:" + bucketid);
        String keyRoutingObject = key.getInstrumentId().toString();
        if (!keyRoutingObject.equalsIgnoreCase(blackBoardRoutingObject)) {
          throw new TestException(
              "Expected same routing objects for the entries in this bucket id "
                  + bucketid + "but got different values "
                  + blackBoardRoutingObject + " and " + keyRoutingObject);
        }
        else {
          Log.getLogWriter().info(
              "Got the expected values "
                  + blackBoardRoutingObject
                  + " and "
                  + keyRoutingObject
                  + " for the keys "
                  + ParRegBB.getBB().getSharedMap().get(
                      "RoutingObjectKeyBucketid:" + bucketid) + " and " + key);
        }
      }
    }
  }

  /**
   * Task to verify that there is only a single bucket id for a routing Object
   * 
   */
  protected static void verifyUniqueBucketForCustomPartioning(int bucketId) {

    if (bucketId == 0) {
      Log
          .getLogWriter()
          .info(
              "This is the first bucket, so no validation required as there is no bucket to be compared");
      return;
    }
    else {
      for (int i = 0; i < bucketId; i++) {
        if (!(ParRegBB.getBB().getSharedMap().get(
            "RoutingObjectForBucketid:" + i) == null)) {
          String referenceValue = (String)ParRegBB.getBB().getSharedMap().get(
              "RoutingObjectForBucketid:" + i);
          String currentValue = (String)ParRegBB.getBB().getSharedMap().get(
              "RoutingObjectForBucketid:" + bucketId);
          Log.getLogWriter().info("currentValue: " + currentValue);
          Log.getLogWriter().info("referenceValue: " + referenceValue);

          if (currentValue.equalsIgnoreCase(referenceValue)) {
            throw new TestException("Two buckets with the id " + i + " and "
                + bucketId + " have the same routing Object " + referenceValue);
          }
          else {
            Log.getLogWriter().info(
                "As expected the bucket with ids " + i + " and " + bucketId
                    + " have the different routing Object " + currentValue
                    + " and " + referenceValue);
          }

        }

      }
    }

  }

}
